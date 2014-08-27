''' CFFI interface to libpq the library '''

import os.path

from cffi import FFI

from psycopg2cffi import _config


ffi = FFI()

# order and comments taken from libpq (ctypes impl)

ffi.cdef('''

// postgres_ext.h

typedef unsigned int Oid;

// libpq-fe.h

typedef enum
{
 /*
  * Although it is okay to add to this list, values which become unused
  * should never be removed, nor should constants be redefined - that would
  * break compatibility with existing code.
  */
 CONNECTION_OK,
 CONNECTION_BAD,
 /* Non-blocking mode only below here */

 /*
  * The existence of these should never be relied upon - they should only
  * be used for user feedback or similar purposes.
  */
 CONNECTION_STARTED,   /* Waiting for connection to be made.  */
 CONNECTION_MADE,   /* Connection OK; waiting to send.    */
 CONNECTION_AWAITING_RESPONSE,  /* Waiting for a response from the
           * postmaster.    */
 CONNECTION_AUTH_OK,   /* Received authentication; waiting for
         * backend startup. */
 CONNECTION_SETENV,   /* Negotiating environment. */
 CONNECTION_SSL_STARTUP,  /* Negotiating SSL. */
 CONNECTION_NEEDED   /* Internal state: connect() needed */
} ConnStatusType;

typedef enum
{
 PGRES_POLLING_FAILED = 0,
 PGRES_POLLING_READING,  /* These two indicate that one may   */
 PGRES_POLLING_WRITING,  /* use select before polling again.   */
 PGRES_POLLING_OK,
 PGRES_POLLING_ACTIVE  /* unused; keep for awhile for backwards
         * compatibility */
} PostgresPollingStatusType;

typedef enum
{
 PGRES_EMPTY_QUERY = 0,  /* empty query string was executed */
 PGRES_COMMAND_OK,   /* a query command that doesn't return
         * anything was executed properly by the
         * backend */
 PGRES_TUPLES_OK,   /* a query command that returns tuples was
         * executed properly by the backend, PGresult
         * contains the result tuples */
 PGRES_COPY_OUT,    /* Copy Out data transfer in progress */
 PGRES_COPY_IN,    /* Copy In data transfer in progress */
 PGRES_BAD_RESPONSE,   /* an unexpected response was recv'd from the
         * backend */
 PGRES_NONFATAL_ERROR,  /* notice or warning message */
 PGRES_FATAL_ERROR,   /* query failed */
} ExecStatusType;

typedef enum
{
 PQTRANS_IDLE,    /* connection idle */
 PQTRANS_ACTIVE,    /* command in progress */
 PQTRANS_INTRANS,   /* idle, within transaction block */
 PQTRANS_INERROR,   /* idle, within failed transaction */
 PQTRANS_UNKNOWN    /* cannot determine status */
} PGTransactionStatusType;

typedef ... PGconn;
typedef ... PGresult;
typedef ... PGcancel;

typedef struct pgNotify
{
    char       *relname;  /* notification condition name */
    int        be_pid;    /* process ID of notifying server process */
    char       *extra;    /* notification parameter */
    ...;
} PGnotify;

// Database connection control functions

extern PGconn *PQconnectdb(const char *conninfo);
extern PGconn *PQconnectStart(const char *conninfo);
extern /*PostgresPollingStatusType*/ int PQconnectPoll(PGconn *conn);
extern void PQfinish(PGconn *conn);

// Connection status functions

extern /*ConnStatusType*/ int PQstatus(const PGconn *conn);
extern /*PGTransactionStatusType*/ int PQtransactionStatus(const PGconn *conn);
extern const char *PQparameterStatus(const PGconn *conn, const char *paramName);
extern int PQprotocolVersion(const PGconn *conn);
extern int PQserverVersion(const PGconn *conn);
extern char *PQerrorMessage(const PGconn *conn);
extern int PQsocket(const PGconn *conn);
extern int PQbackendPID(const PGconn *conn);

// Command execution functions

extern PGresult *PQexec(PGconn *conn, const char *query);
extern /*ExecStatusType*/ int PQresultStatus(const PGresult *res);
extern char *PQresultErrorMessage(const PGresult *res);
extern char *PQresultErrorField(const PGresult *res, int fieldcode);
extern void PQclear(PGresult *res);

// Retrieving query result information

extern int PQntuples(const PGresult *res);
extern int PQnfields(const PGresult *res);
extern char *PQfname(const PGresult *res, int field_num);
extern Oid PQftype(const PGresult *res, int field_num);
extern int PQfsize(const PGresult *res, int field_num);
extern int PQfmod(const PGresult *res, int field_num);
extern int PQgetisnull(const PGresult *res, int tup_num, int field_num);
extern int PQgetlength(const PGresult *res, int tup_num, int field_num);
extern char *PQgetvalue(const PGresult *res, int tup_num, int field_num);

// Retrieving other result information

extern char *PQcmdStatus(PGresult *res);
extern char *PQcmdTuples(PGresult *res);
extern Oid PQoidValue(const PGresult *res); /* new and improved */

''')

if _config.PG_VERSION >= 0x090000:
    ffi.cdef('''
// Escaping string for inclusion in sql commands
extern char *PQescapeLiteral(PGconn *conn, const char *str, size_t len);
    ''')

ffi.cdef('''
// Escaping string for inclusion in sql commands
extern size_t PQescapeStringConn(PGconn *conn,
    char *to, const char *from, size_t length,
    int *error);
extern size_t PQescapeString(char *to, const char *from, size_t length);
extern unsigned char *PQescapeByteaConn(PGconn *conn,
    const unsigned char *from, size_t from_length,
    size_t *to_length);
extern unsigned char *PQescapeBytea(const unsigned char *from, size_t from_length,
    size_t *to_length);
extern unsigned char *PQunescapeBytea(const unsigned char *strtext,
    size_t *retbuflen);

// Asynchronous Command Processing

extern int PQsendQuery(PGconn *conn, const char *query);
extern PGresult *PQgetResult(PGconn *conn);
extern int PQconsumeInput(PGconn *conn);
extern int PQisBusy(PGconn *conn);
extern int PQsetnonblocking(PGconn *conn, int arg);
extern int PQflush(PGconn *conn);

// Cancelling queries in progress

extern PGcancel *PQgetCancel(PGconn *conn);
extern void PQfreeCancel(PGcancel *cancel);
extern int PQcancel(PGcancel *cancel, char *errbuf, int errbufsize);
extern int PQrequestCancel(PGconn *conn);

// Functions Associated with the COPY Command

extern int PQgetCopyData(PGconn *conn, char **buffer, int async);
extern int PQputCopyEnd(PGconn *conn, const char *errormsg);
extern int PQputCopyData(PGconn *conn, const char *buffer, int nbytes);

// Miscellaneous functions

extern void PQfreemem(void *ptr);

// Notice processing

typedef void (*PQnoticeProcessor) (void *arg, const char *message);
extern PQnoticeProcessor PQsetNoticeProcessor(PGconn *conn,
    PQnoticeProcessor proc,
    void *arg);
extern PGnotify *PQnotifies(PGconn *conn);

// Large object
extern int lo_open(PGconn *conn, Oid lobjId, int mode);
extern Oid lo_create(PGconn *conn, Oid lobjId);
extern Oid lo_import(PGconn *conn, const char *filename);
extern int lo_read(PGconn *conn, int fd, char *buf, size_t len);
extern int lo_write(PGconn *conn, int fd, const char *buf, size_t len);
extern int lo_tell(PGconn *conn, int fd);
extern int lo_lseek(PGconn *conn, int fd, int offset, int whence);
extern int lo_close(PGconn *conn, int fd);
extern int lo_unlink(PGconn *conn, Oid lobjId);
extern int lo_export(PGconn *conn, Oid lobjId, const char *filename);
extern int lo_truncate(PGconn *conn, int fd, size_t len);

''')


libpq = ffi.verify('''
#include <postgres_ext.h>
#include <libpq-fe.h>
        ''',
        libraries=['pq'],
        library_dirs=[os.path.dirname(_config.PG_LIBRARY)],
        include_dirs=[_config.PG_INCLUDE_DIR] \
                if _config.PG_INCLUDE_DIR else [],
        ext_package='psycopg2cffi')


# imported from postgres/src/include/postgres_ext.h

libpq.PG_DIAG_SEVERITY        = ord('S')
libpq.PG_DIAG_SQLSTATE        = ord('C')
libpq.PG_DIAG_MESSAGE_PRIMARY = ord('M')
libpq.PG_DIAG_MESSAGE_DETAIL  = ord('D')
libpq.PG_DIAG_MESSAGE_HINT    = ord('H')
libpq.PG_DIAG_STATEMENT_POSITION = ord('P')
libpq.PG_DIAG_INTERNAL_POSITION = ord('p')
libpq.PG_DIAG_INTERNAL_QUERY  = ord('q')
libpq.PG_DIAG_CONTEXT         = ord('W')
libpq.PG_DIAG_SCHEMA_NAME     = ord('s')
libpq.PG_DIAG_TABLE_NAME      = ord('t')
libpq.PG_DIAG_COLUMN_NAME     = ord('c')
libpq.PG_DIAG_DATATYPE_NAME   = ord('d')
libpq.PG_DIAG_CONSTRAINT_NAME = ord('n')
libpq.PG_DIAG_SOURCE_FILE     = ord('F')
libpq.PG_DIAG_SOURCE_LINE     = ord('L')
libpq.PG_DIAG_SOURCE_FUNCTION = ord('R')

