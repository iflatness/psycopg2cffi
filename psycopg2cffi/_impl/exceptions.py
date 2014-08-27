from psycopg2cffi._impl.libpq import libpq, ffi


class Warning(Exception):
    pass


class Error(Exception):
    pgerror = None
    pgcode = None
    cursor = None
    _pgres = None

    @property
    def diag(self):
        return Diagnostics(self)

    def __del__(self):
        if self._pgres:
            libpq.PQclear(self._pgres)
            self._pgres = None

    def __reduce__(self):
        t = super(Error, self).__reduce__()
        if not isinstance(t, tuple):
            return t
        # note: in c implementation reduce returns a 2-items tuple;
        # in python a 3-items tuple. Maybe the c exception doesn't have a dict?
        if len(t) != 3:
            return t

        d = t[2].copy()
        d.pop('cursor', None)
        d.pop('_pgres', None)
        return (t[0], t[1], d)

    def __setstate__(self, state):
        self.pgerror = state.get('pgerror')
        self.pgcode = state.get('pgcode')


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


class QueryCanceledError(OperationalError):
    pass


class TransactionRollbackError(OperationalError):
    pass


class Diagnostics(object):
    def __init__(self, exc):
        self._exc = exc

    def _get_field(self, field):
        if self._exc and self._exc._pgres:
            b = libpq.PQresultErrorField(self._exc._pgres, field)
            if b:
                b = ffi.string(b)
                if not isinstance(b, str):
                    # Can't choose encoding :(
                    b = b.decode()
                return b

    @property
    def severity(self):
        return self._get_field(libpq.PG_DIAG_SEVERITY)

    @property
    def sqlstate(self):
        return self._get_field(libpq.PG_DIAG_SQLSTATE)

    @property
    def message_primary(self):
        return self._get_field(libpq.PG_DIAG_MESSAGE_PRIMARY)

    @property
    def message_detail(self):
        return self._get_field(libpq.PG_DIAG_MESSAGE_DETAIL)

    @property
    def message_hint(self):
        return self._get_field(libpq.PG_DIAG_MESSAGE_HINT)

    @property
    def statement_position(self):
        return self._get_field(libpq.PG_DIAG_STATEMENT_POSITION)

    @property
    def internal_position(self):
        return self._get_field(libpq.PG_DIAG_INTERNAL_POSITION)

    @property
    def internal_query(self):
        return self._get_field(libpq.PG_DIAG_INTERNAL_QUERY)

    @property
    def context(self):
        return self._get_field(libpq.PG_DIAG_CONTEXT)

    @property
    def schema_name(self):
        return self._get_field(libpq.PG_DIAG_SCHEMA_NAME)

    @property
    def table_name(self):
        return self._get_field(libpq.PG_DIAG_TABLE_NAME)

    @property
    def column_name(self):
        return self._get_field(libpq.PG_DIAG_COLUMN_NAME)

    @property
    def datatype_name(self):
        return self._get_field(libpq.PG_DIAG_DATATYPE_NAME)

    @property
    def constraint_name(self):
        return self._get_field(libpq.PG_DIAG_CONSTRAINT_NAME)

    @property
    def source_file(self):
        return self._get_field(libpq.PG_DIAG_SOURCE_FILE)

    @property
    def source_line(self):
        return self._get_field(libpq.PG_DIAG_SOURCE_LINE)

    @property
    def source_function(self):
        return self._get_field(libpq.PG_DIAG_SOURCE_FUNCTION)

