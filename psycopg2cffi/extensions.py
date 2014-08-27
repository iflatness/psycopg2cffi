"""psycopg extensions to the DBAPI-2.0

This module holds all the extensions to the DBAPI-2.0 provided by psycopg.

- `connection` -- the new-type inheritable connection class
- `cursor` -- the new-type inheritable cursor class
- `lobject` -- the new-type inheritable large object class
- `adapt()` -- exposes the PEP-246_ compatible adapting mechanism used
  by psycopg to adapt Python types to PostgreSQL ones

.. _PEP-246: http://www.python.org/peps/pep-0246.html
"""
# psycopg/extensions.py - DBAPI-2.0 extensions specific to psycopg
#
# Copyright (C) 2003-2010 Federico Di Gregorio  <fog@debian.org>
#
# psycopg2 is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# In addition, as a special exception, the copyright holders give
# permission to link this program with the OpenSSL library (or with
# modified versions of OpenSSL that use the same license as OpenSSL),
# and distribute linked combinations including the two.
#
# You must obey the GNU Lesser General Public License in all respects for
# all of the code used other than OpenSSL.
#
# psycopg2 is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.
import sys as _sys

from psycopg2cffi._impl import connection as _connection
from psycopg2cffi._impl.adapters import adapt, adapters, b, s, u
from psycopg2cffi._impl.adapters import Binary, Boolean, Int, Float
from psycopg2cffi._impl.adapters import QuotedString, AsIs, ISQLQuote
from psycopg2cffi._impl.connection import Connection as connection
from psycopg2cffi._impl.consts import *
from psycopg2cffi._impl.cursor import Cursor as cursor
from psycopg2cffi._impl.encodings import encodings
from psycopg2cffi._impl.exceptions import QueryCanceledError
from psycopg2cffi._impl.exceptions import TransactionRollbackError
from psycopg2cffi._impl.exceptions import Diagnostics
from psycopg2cffi._impl.lobject import LargeObject as lobject
from psycopg2cffi._impl.notify import Notify
from psycopg2cffi._impl.typecasts import (
    UNICODE, INTEGER, LONGINTEGER, BOOLEAN, FLOAT, TIME, DATE, INTERVAL,
    DECIMAL,
    BINARYARRAY, BOOLEANARRAY, DATEARRAY, DATETIMEARRAY, DECIMALARRAY,
    FLOATARRAY, INTEGERARRAY, INTERVALARRAY, LONGINTEGERARRAY, ROWIDARRAY,
    STRINGARRAY, TIMEARRAY, UNICODEARRAY)
from psycopg2cffi._impl.typecasts import string_types, binary_types
from psycopg2cffi._impl.typecasts import new_type, new_array_type, register_type
from psycopg2cffi._impl.xid import Xid

# These are looked for by the test suite to make the tests run
from psycopg2cffi._impl.typecasts import DATETIME as PYDATETIME
PYDATE = DATE
PYTIME = TIME
PYINTERVAL = INTERVAL

def register_adapter(typ, callable):
    """Register 'callable' as an ISQLQuote adapter for type 'typ'."""
    adapters[(typ, ISQLQuote)] = callable


# The SQL_IN class is the official adapter for tuples starting from 2.0.6.
class SQL_IN(object):
    """Adapt any iterable to an SQL quotable object."""
    def __init__(self, seq):
        self._seq = seq
        self._conn = None

    def prepare(self, conn):
        self._conn = conn

    def getquoted(self):
        # this is the important line: note how every object in the
        # list is adapted and then how getquoted() is called on it
        pobjs = [adapt(o) for o in self._seq]
        if self._conn is not None:
            for obj in pobjs:
                if hasattr(obj, 'prepare'):
                    obj.prepare(self._conn)
        qobjs = [o.getquoted() for o in pobjs]
        return b('(') + b(', ').join(qobjs) + b(')')

    def __str__(self):
        return str(self.getquoted())


class NoneAdapter(object):
    """Adapt None to NULL.

    This adapter is not used normally as a fast path in mogrify uses NULL,
    but it makes easier to adapt composite types.
    """
    def __init__(self, obj):
        pass

    def getquoted(self, _null=b("NULL")):
        return _null


# Create default json typecasters for PostgreSQL 9.2 oids
from psycopg2cffi._json import register_default_json

try:
    JSON, JSONARRAY = register_default_json()
except ImportError:
    pass

del register_default_json


# Create default Range typecasters
from psycopg2cffi._range import Range
del Range

def set_wait_callback(f):
    _connection._green_callback = f


def get_wait_callback():
    return _connection._green_callback
