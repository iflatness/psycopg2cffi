import threading
import weakref
from functools import wraps

from psycopg2cffi._impl import consts
from psycopg2cffi._impl import encodings as _enc
from psycopg2cffi._impl import exceptions
from psycopg2cffi._impl.libpq import libpq, ffi
from psycopg2cffi._impl import util
from psycopg2cffi._impl.cursor import Cursor
from psycopg2cffi._impl.lobject import LargeObject
from psycopg2cffi._impl.notify import Notify
from psycopg2cffi._impl.xid import Xid


# Map between isolation levels names and values and back.
_isolevels = {
    '':                 consts.ISOLATION_LEVEL_AUTOCOMMIT,
    'read uncommitted': consts.ISOLATION_LEVEL_READ_UNCOMMITTED,
    'read committed':   consts.ISOLATION_LEVEL_READ_COMMITTED,
    'repeatable read':  consts.ISOLATION_LEVEL_REPEATABLE_READ,
    'serializable':     consts.ISOLATION_LEVEL_SERIALIZABLE,
    'default':         -1,
}

for k, v in list(_isolevels.items()):
    _isolevels[v] = k

del k, v

_green_callback = None


def check_closed(func):
    @wraps(func)
    def check_closed_(self, *args, **kwargs):
        if self.closed:
            raise exceptions.InterfaceError('connection already closed')
        return func(self, *args, **kwargs)
    return check_closed_


def check_notrans(func):
    @wraps(func)
    def check_notrans_(self, *args, **kwargs):
        if self.status != consts.STATUS_READY:
            raise exceptions.ProgrammingError('not valid in transaction')
        return func(self, *args, **kwargs)
    return check_notrans_


def check_tpc(func):
    @wraps(func)
    def check_tpc_(self, *args, **kwargs):
        if self._tpc_xid:
            raise exceptions.ProgrammingError(
                '%s cannot be used during a two-phase transaction'
                % func.__name__)
        return func(self, *args, **kwargs)
    return check_tpc_


def check_tpc_supported(func):
    @wraps(func)
    def check_tpc_supported_(self, *args, **kwargs):
        if self.server_version < 80100:
            raise exceptions.NotSupportedError(
                "server version %s: two-phase transactions not supported"
                % self.server_version)
        return func(self, *args, **kwargs)
    return check_tpc_supported_


def check_async(func):
    @wraps(func)
    def check_async_(self, *args, **kwargs):
        if self._async:
            raise exceptions.ProgrammingError(
                '%s cannot be used in asynchronous mode' % func.__name__)
        return func(self, *args, **kwargs)
    return check_async_


class Connection(object):

    # Various exceptions which should be accessible via the Connection
    # class according to dbapi 2.0
    Error = exceptions.Error
    DatabaseError = exceptions.DatabaseError
    IntegrityError = exceptions.IntegrityError
    InterfaceError = exceptions.InterfaceError
    InternalError = exceptions.InternalError
    NotSupportedError = exceptions.NotSupportedError
    OperationalError = exceptions.OperationalError
    ProgrammingError = exceptions.ProgrammingError
    Warning = exceptions.Warning

    def __init__(self, dsn, async=False):

        self.dsn = dsn
        self.status = consts.STATUS_SETUP
        self._encoding = None

        self._closed = 0
        self._cancel = ffi.NULL
        self._typecasts = {}
        self._tpc_xid = None
        self._notifies = []
        self._autocommit = False
        self._pgconn = None
        self._equote = False
        self._lock = threading.RLock()
        self.notices = []
        self.cursor_factory = None

        # The number of commits/rollbacks done so far
        self._mark = 0

        self._async = async
        self._async_status = consts.ASYNC_DONE
        self._async_cursor = None

        self_ref = weakref.ref(self)
        self._notice_callback = ffi.callback(
            'void(void *, const char *)',
            lambda arg, message: self_ref()._process_notice(
                arg, ffi.string(message).decode()))

        if not self._async:
            self._connect_sync()
        else:
            self._connect_async()

    def _connect_sync(self):
        self._pgconn = libpq.PQconnectdb(self.dsn.encode('utf-8'))
        if not self._pgconn:
            raise exceptions.OperationalError('PQconnectdb() failed')
        elif libpq.PQstatus(self._pgconn) == libpq.CONNECTION_BAD:
            raise self._create_exception()

        # Register notice processor
        libpq.PQsetNoticeProcessor(
                self._pgconn, self._notice_callback, ffi.NULL)

        self.status = consts.STATUS_READY
        self._setup()

    def _connect_async(self):
        """Create an async connection.

        The connection will be completed banging on poll():
        First with self._conn_poll_connecting() that will finish connection,
        then with self._poll_setup_async() that will do the same job
        of self._setup().

        """
        self._pgconn = libpq.PQconnectStart(self.dsn.encode())
        if not self._pgconn:
            raise exceptions.OperationalError('PQconnectStart() failed')
        elif libpq.PQstatus(self._pgconn) == libpq.CONNECTION_BAD:
            raise self._create_exception()

        libpq.PQsetNoticeProcessor(
                self._pgconn, self._notice_callback, ffi.NULL)

    def __del__(self):
        self._close()

    @check_closed
    def __enter__(self):
        return self

    def __exit__(self, type, name, tb):
        if type is None:
            self.commit()
        else:
            self.rollback()

    def close(self):
        return self._close()

    @check_closed
    @check_async
    @check_tpc
    def rollback(self):
        self._rollback()

    @check_closed
    @check_async
    @check_tpc
    def commit(self):
        self._commit()

    @check_closed
    @check_async
    def reset(self):
        with self._lock:
            self._execute_command(
                "ABORT; RESET ALL; SET SESSION AUTHORIZATION DEFAULT;")
            self.status = consts.STATUS_READY
            self._mark += 1
            self._autocommit = False
            self._tpc_xid = None

    def _get_guc(self, name):
        """Return the value of a configuration parameter."""
        with self._lock:
            query = 'SHOW %s' % name

            if _green_callback:
                pgres = self._execute_green(query)
            else:
                if not isinstance(query, bytes):
                    query = query.encode()
                pgres = libpq.PQexec(self._pgconn, query)

            if not pgres or libpq.PQresultStatus(pgres) != libpq.PGRES_TUPLES_OK:
                raise exceptions.OperationalError("can't fetch %s" % name)
            rv = ffi.string(libpq.PQgetvalue(pgres, 0, 0)).decode()
            libpq.PQclear(pgres)
            return rv

    def _set_guc(self, name, value):
        """Set the value of a configuration parameter."""
        if value.lower() != 'default':
            value = util.quote_string(self, value)
        self._execute_command('SET %s TO %s' % (name, value))

    def _set_guc_onoff(self, name, value):
        """Set the value of a configuration parameter to a boolean.

        The string 'default' is accepted too.
        """
        if isinstance(value, bytes) and not isinstance(value, str):
            value = value.decode()
        if isinstance(value, str) and value.lower() == 'default':
            value = 'default'
        else:
            value = value and 'on' or 'off'
        self._set_guc(name, value)

    @property
    @check_closed
    def isolation_level(self):
        if self._autocommit:
            return consts.ISOLATION_LEVEL_AUTOCOMMIT
        else:
            name = self._get_guc('default_transaction_isolation')
            return _isolevels[name.lower()]

    @check_async
    def set_isolation_level(self, level):
        if level < 0 or level > 4:
            raise ValueError('isolation level must be between 0 and 4')

        prev = self.isolation_level
        if prev == level:
            return

        self._rollback()
        if level == consts.ISOLATION_LEVEL_AUTOCOMMIT:
            return self.set_session(autocommit=True)
        else:
            return self.set_session(isolation_level=level, autocommit=False)

    @check_closed
    @check_notrans
    def set_session(self, isolation_level=None, readonly=None, deferrable=None,
                    autocommit=None):
        if isolation_level is not None:
            if isinstance(isolation_level, int):
                if isolation_level < 1 or isolation_level > 4:
                    raise ValueError('isolation level must be between 1 and 4')
                isolation_level = _isolevels[isolation_level]
            elif isinstance(isolation_level, str):
                isolation_level = isolation_level.lower()
                if not isolation_level or isolation_level not in _isolevels:
                    raise ValueError("bad value for isolation level: '%s'" %
                        isolation_level)
            elif isinstance(isolation_level, bytes):
                isolation_level = isolation_level.decode().lower()
                if not isolation_level or isolation_level not in _isolevels:
                    raise ValueError("bad value for isolation level: '%s'" %
                        isolation_level)
            else:
                raise TypeError("bad isolation level: '%r'" % isolation_level)

            if self.server_version < 80000:
                if isolation_level == 'read uncommitted':
                    isolation_level = 'read committed'
                elif isolation_level == 'repeatable read':
                    isolation_level = 'serializable'

            self._set_guc("default_transaction_isolation", isolation_level)

        if readonly is not None:
            self._set_guc_onoff('default_transaction_read_only', readonly)

        if deferrable is not None:
            self._set_guc_onoff('default_transaction_deferrable', deferrable)

        if autocommit is not None:
            self._autocommit = bool(autocommit)

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        self.set_session(autocommit=value)

    @property
    def async(self):
        return self._async

    @check_closed
    def get_backend_pid(self):
        return libpq.PQbackendPID(self._pgconn)

    def get_parameter_status(self, parameter):
        p = libpq.PQparameterStatus(self._pgconn, parameter.encode())
        return ffi.string(p) if p != ffi.NULL else None

    def get_transaction_status(self):
        return libpq.PQtransactionStatus(self._pgconn)

    def cursor(self, name=None, cursor_factory=None,
            withhold=False, scrollable=None):
        if cursor_factory is None:
            cursor_factory = self.cursor_factory or Cursor

        cur = cursor_factory(self, name)

        if not isinstance(cur, Cursor):
            raise TypeError(
                "cursor factory must be subclass of %s" %
                '.'.join([Cursor.__module__, Cursor.__name__]))

        if withhold:
            cur.withhold = withhold

        if scrollable is not None:
            cur.scrollable = scrollable

        if name and self._async:
            raise exceptions.ProgrammingError(
                "asynchronous connections cannot produce named cursors")

        cur._mark = self._mark
        return cur

    @check_closed
    @check_tpc
    def cancel(self):
        err_length = 256
        errbuf = ffi.new('char[]', err_length)
        if libpq.PQcancel(self._cancel, errbuf, err_length) == 0:
            raise exceptions.OperationalError(ffi.string(errbuf))

    def isexecuting(self):
        if not self._async:
            return False

        if self.status != consts.STATUS_READY:
            return True

        if self._async_cursor is not None:
            return True

        return False

    @property
    def encoding(self):
        return self._encoding

    @check_closed
    @check_async
    def set_client_encoding(self, encoding):
        encoding = _enc.normalize(encoding)
        if self.encoding == encoding:
            return

        pyenc = _enc.encodings[encoding]
        self._rollback()
        self._set_guc('client_encoding', encoding)
        self._encoding = encoding
        self._py_enc = pyenc

    @property
    def notifies(self):
        return self._notifies

    @property
    @check_closed
    def protocol_version(self):
        return libpq.PQprotocolVersion(self._pgconn)

    @property
    @check_closed
    def server_version(self):
        return libpq.PQserverVersion(self._pgconn)

    def fileno(self):
        return libpq.PQsocket(self._pgconn)

    @property
    def closed(self):
        return self._closed

    @check_closed
    @check_tpc_supported
    def xid(self, format_id, gtrid, bqual):
        return Xid(format_id, gtrid, bqual)

    @check_closed
    @check_async
    @check_tpc_supported
    def tpc_begin(self, xid):
        if not isinstance(xid, Xid):
            xid = Xid.from_string(xid)

        if self.status != consts.STATUS_READY:
            raise exceptions.ProgrammingError(
                'tpc_begin must be called outside a transaction')

        if self._autocommit:
            raise exceptions.ProgrammingError(
                "tpc_begin can't be called in autocommit mode")

        self._begin_transaction()
        self._tpc_xid = xid

    @check_closed
    @check_async
    @check_tpc_supported
    def tpc_commit(self, xid=None):
        self._finish_tpc('COMMIT PREPARED', self._commit, xid)

    @check_closed
    @check_async
    @check_tpc_supported
    def tpc_rollback(self, xid=None):
        self._finish_tpc('ROLLBACK PREPARED', self._rollback, xid)

    @check_closed
    @check_async
    def tpc_prepare(self):
        if not self._tpc_xid:
            raise exceptions.ProgrammingError(
                'prepare must be called inside a two-phase transaction')

        self._execute_tpc_command('PREPARE TRANSACTION', self._tpc_xid)
        self.status = consts.STATUS_PREPARED

    @check_closed
    @check_async
    @check_tpc_supported
    def tpc_recover(self):
        return Xid.tpc_recover(self)

    def lobject(self, oid=0, mode='', new_oid=0, new_file=None,
                lobject_factory=LargeObject):
        obj = lobject_factory(self, oid, mode, new_oid, new_file)
        return obj

    def poll(self):
        if self.status == consts.STATUS_SETUP:
            self.status = consts.STATUS_CONNECTING
            return consts.POLL_WRITE

        if self.status == consts.STATUS_CONNECTING:
            res = self._poll_connecting()
            if res == consts.POLL_OK and self._async:
                return self._poll_setup_async()
            return res

        if self.status in (consts.STATUS_READY, consts.STATUS_BEGIN,
                           consts.STATUS_PREPARED):
            res = self._poll_query()

            if res == consts.POLL_OK and self._async and self._async_cursor:

                # Get the cursor object from the weakref
                curs = self._async_cursor()
                if curs is None:
                    util.pq_clear_async(self._pgconn)
                    raise exceptions.InterfaceError(
                        "the asynchronous cursor has disappeared")

                libpq.PQclear(curs._pgres)

                curs._pgres = util.pq_get_last_result(self._pgconn)
                try:
                    curs._pq_fetch()
                finally:
                    self._async_cursor = None
            return res

        return consts.POLL_ERROR

    def _poll_connecting(self):
        """poll during a connection attempt until the connection has
        established.

        """
        status_map = {
            libpq.PGRES_POLLING_OK: consts.POLL_OK,
            libpq.PGRES_POLLING_READING: consts.POLL_READ,
            libpq.PGRES_POLLING_WRITING: consts.POLL_WRITE,
            libpq.PGRES_POLLING_FAILED: consts.POLL_ERROR,
            libpq.PGRES_POLLING_ACTIVE: consts.POLL_ERROR,
        }
        res = status_map.get(libpq.PQconnectPoll(self._pgconn), None)

        if res is None:
            return consts.POLL_ERROR
        elif res == consts.POLL_ERROR:
            raise self._create_exception()
        return res

    def _poll_query(self):
        """Poll the connection for the send query/retrieve result phase

        Advance the async_status (usually going WRITE -> READ -> DONE) but
        don't mess with the connection status.

        """
        if self._async_status == consts.ASYNC_WRITE:
            ret = self._poll_advance_write(libpq.PQflush(self._pgconn))

        elif self._async_status == consts.ASYNC_READ:
            if self._async:
                ret = self._poll_advance_read(self._is_busy())
            else:
                ret = self._poll_advance_read(self._is_busy())

        elif self._async_status == consts.ASYNC_DONE:
            ret = self._poll_advance_read(self._is_busy())

        else:
            ret = consts.POLL_ERROR

        return ret

    def _poll_advance_write(self, flush):
        """Advance to the next state after an attempt of flushing output"""
        if flush == 0:
            self._async_status = consts.ASYNC_READ
            return consts.POLL_READ

        if flush == 1:
            return consts.POLL_WRITE

        if flush == -1:
            raise self._create_exception()

        return consts.POLL_ERROR

    def _poll_advance_read(self, busy):
        """Advance to the next state after a call to a _is_busy* method"""
        if busy == 0:
            self._async_status = consts.ASYNC_DONE
            return consts.POLL_OK

        if busy == 1:
            return consts.POLL_READ

        return consts.POLL_ERROR

    def _poll_setup_async(self):
        """Advance to the next state during an async connection setup

        If the connection is green, this is performed by the regular sync
        code so the queries are sent by conn_setup() while in
        CONN_STATUS_READY state.

        """
        if self.status == consts.STATUS_CONNECTING:
            util.pq_set_non_blocking(self._pgconn, 1, True)

            self._equote = self._get_equote()
            self._get_encoding()
            self._have_cancel_key()

            self._autocommit = True

            # If the current datestyle is not compatible (not ISO) then
            # force it to ISO
            if not self._iso_compatible_datestyle():
                self.status = consts.STATUS_DATESTYLE

                if libpq.PQsendQuery(self._pgconn, "SET DATESTYLE TO 'ISO'"):
                    self._async_status = consts.ASYNC_WRITE
                    return consts.POLL_WRITE
                else:
                    raise self._create_exception()

            self.status = consts.STATUS_READY
            return consts.POLL_OK

        if self.status == consts.STATUS_DATESTYLE:
            res = self._poll_query()
            if res != consts.POLL_OK:
                return res

            pgres = util.pq_get_last_result(self._pgconn)
            if not pgres or \
                libpq.PQresultStatus(pgres) != libpq.PGRES_COMMAND_OK:
                raise exceptions.OperationalError("can't set datetyle to ISO")
            libpq.PQclear(pgres)

            self.status = consts.STATUS_READY
            return consts.POLL_OK

        return consts.POLL_ERROR

    def _setup(self):
        self._equote = self._get_equote()
        self._get_encoding()
        self._have_cancel_key()

        with self._lock:
            # If the current datestyle is not compatible (not ISO) then
            # force it to ISO
            if not self._iso_compatible_datestyle():
                self.status = consts.STATUS_DATESTYLE
                self._set_guc('datestyle', 'ISO')

            self._closed = 0

    def _have_cancel_key(self):
        if self._cancel != ffi.NULL:
            tmp, self._cancel = self._cancel, ffi.NULL
            libpq.PQfreeCancel(tmp)

        self._cancel = libpq.PQgetCancel(self._pgconn)
        if self._cancel == ffi.NULL:
            raise exceptions.OperationalError("can't get cancellation key")

    def _begin_transaction(self):
        if self.status == consts.STATUS_READY and not self._autocommit:
            self._execute_command(b'BEGIN')
            self.status = consts.STATUS_BEGIN

    def _execute_command(self, command):
        with self._lock:
            if _green_callback:
                pgres = self._execute_green(command)
            else:
                if not isinstance(command, bytes):
                    command = command.encode()
                pgres = libpq.PQexec(self._pgconn, command)

            if not pgres:
                raise self._create_exception()
            try:
                pgstatus = libpq.PQresultStatus(pgres)
                if pgstatus != libpq.PGRES_COMMAND_OK:
                    exc = self._create_exception(pgres=pgres)
                    pgres = None    # ownership transferred to exc
                    raise exc
            finally:
                if pgres:
                    libpq.PQclear(pgres)

    def _execute_tpc_command(self, command, xid):
        cmd = '%s %s' % (command, util.quote_string(self, str(xid)))
        self._execute_command(cmd)
        self._mark += 1

    def _execute_green(self, query):
        """Execute version for green threads"""
        if self._async_cursor:
            raise exceptions.ProgrammingError(
                "a single async query can be executed on the same connection")

        self._async_cursor = True

        if not isinstance(query, bytes):
            query = query.encode(self.encoding)
        if not libpq.PQsendQuery(self._pgconn, query):
            self._async_cursor = None
            return

        self._async_status = consts.ASYNC_WRITE

        try:
            _green_callback(self)
        except Exception:
            self.close()
            raise
        else:
            return util.pq_get_last_result(self._pgconn)
        finally:
            self._async_cursor = None
            self._async_status = consts.ASYNC_DONE

    def _finish_tpc(self, command, fallback, xid):
        if xid:
            # committing/aborting a received transaction.
            if self.status != consts.STATUS_READY:
                raise exceptions.ProgrammingError(
                    "tpc_commit/tpc_rollback with a xid "
                    "must be called outside a transaction")

            self._execute_tpc_command(command, xid)

        else:
            # committing/aborting our own transaction.
            if not self._tpc_xid:
                raise exceptions.ProgrammingError(
                    "tpc_commit/tpc_rollback with no parameter "
                    "must be called in a two-phase transaction")

            if self.status == consts.STATUS_BEGIN:
                fallback()
            elif self.status == consts.STATUS_PREPARED:
                self._execute_tpc_command(command, self._tpc_xid)
            else:
                raise exceptions.InterfaceError(
                    'unexpected state in tpc_commit/tpc_rollback')

            self.status = consts.STATUS_READY
            self._tpc_xid = None

    def _close(self):
        if self._closed == 1:
            return

        if self._cancel:
            libpq.PQfreeCancel(self._cancel)
            self._cancel = ffi.NULL

        if self._pgconn:
            libpq.PQfinish(self._pgconn)
            self._pgconn = None

        self._closed = 1

    def _commit(self):
        with self._lock:
            if self._autocommit or self.status != consts.STATUS_BEGIN:
                return
            self._mark += 1
            try:
                self._execute_command('COMMIT')
            finally:
                self.status = consts.STATUS_READY

    def _rollback(self):
        with self._lock:
            if self._autocommit or self.status != consts.STATUS_BEGIN:
                return
            self._mark += 1
            try:
                self._execute_command('ROLLBACK')
            finally:
                self.status = consts.STATUS_READY

    def _get_encoding(self):
        """Retrieving encoding"""
        client_encoding = self.get_parameter_status('client_encoding')
        self._encoding = _enc.normalize(client_encoding)
        self._py_enc = _enc.encodings[self._encoding]

    def _get_equote(self):
        ret = libpq.PQparameterStatus(
            self._pgconn, b'standard_conforming_strings')
        return ret and ffi.string(ret) == 'off' or False

    def _is_busy(self):
        with self._lock:
            if libpq.PQconsumeInput(self._pgconn) == 0:
                raise exceptions.OperationalError(
                    ffi.string(libpq.PQerrorMessage(self._pgconn)))
            res = libpq.PQisBusy(self._pgconn)
            self._process_notifies()
            return res

    def _process_notice(self, arg, message):
        """Store the given message in `self.notices`

        Also delete older entries to make sure there are no more then 50
        entries in the list.

        """
        self.notices.append(message)
        length = len(self.notices)
        if length > 50:
            del self.notices[:length - 50]

    def _process_notifies(self):
        while True:
            pg_notify = libpq.PQnotifies(self._pgconn)
            if not pg_notify:
                break

            notify = Notify(
                pg_notify.be_pid,
                ffi.string(pg_notify.relname).decode(),
                ffi.string(pg_notify.extra).decode())
            self._notifies.append(notify)

            libpq.PQfreemem(pg_notify)

    def _create_exception(self, pgres=None, msg=None, cursor=None):
        """Return the appropriate exception instance for the current status.

        IMPORTANT: the new exception takes ownership of pgres: if pgres is
        passed as parameter, the callee must delete its pointer (e.g. it may
        be set to null). If there is a pgres in the cursor it is "stolen": the
        cursor will have it set to Null.

        """
        assert pgres is None or cursor is None, \
            "cannot specify pgres and cursor together"

        if cursor and cursor._pgres:
            pgres, cursor._pgres = cursor._pgres, ffi.NULL

        exc_type = exceptions.OperationalError
        code = pgmsg = None

        # If no custom message is passed then get the message from postgres.
        # If pgres is available then we first try to get the message for the
        # last command, and then the error message for the connection
        if pgres:
            pgmsg = libpq.PQresultErrorMessage(pgres)
            pgmsg = ffi.string(pgmsg) if pgmsg else None

            # Get the correct exception class based on the error code
            code = libpq.PQresultErrorField(pgres, libpq.PG_DIAG_SQLSTATE)
            if code != ffi.NULL:
                code = ffi.string(code)
                if not isinstance(code, str):
                    code = code.decode(self.encoding)
                exc_type = util.get_exception_for_sqlstate(code)
            else:
                code = None
                exc_type = exceptions.DatabaseError

        if not pgmsg:
            pgmsg = libpq.PQerrorMessage(self._pgconn)
            pgmsg = ffi.string(pgmsg) if pgmsg else None
        if not isinstance(pgmsg, str):
            pgmsg = pgmsg.decode(self.encoding)

        if msg is None and pgmsg:
            msg = pgmsg
            for prefix in ("ERROR:  ", "FATAL:  ", "PANIC:  "):
                if msg.startswith(prefix):
                    msg = msg[len(prefix):]
                    break

        # Clear the connection if the status is CONNECTION_BAD (fatal error)
        if self._pgconn and libpq.PQstatus(self._pgconn) == libpq.CONNECTION_BAD:
            self._closed = 2

        exc = exc_type(msg)
        exc.pgcode = code
        exc.pgerror = pgmsg
        exc.cursor = cursor
        exc._pgres = pgres

        return exc

    def _have_wait_callback(self):
        return bool(_green_callback)

    def _iso_compatible_datestyle(self):
        ''' Return whether connection DateStyle is ISO-compatible
        '''
        datestyle = libpq.PQparameterStatus(self._pgconn, b'DateStyle')
        return datestyle != ffi.NULL and \
                ffi.string(datestyle).startswith(b'ISO')


def _connect(dsn, connection_factory=None, async=False):
    if connection_factory is None:
        connection_factory = Connection

    # Mimic the construction method as used by psycopg2, which notes:
    # Here we are breaking the connection.__init__ interface defined
    # by psycopg2. So, if not requiring an async conn, avoid passing
    # the async parameter.
    if async:
        return connection_factory(dsn, async=True)
    else:
        return connection_factory(dsn)

