#!/usr/bin/env python

'''
Code to work with python DB API v2.0 Connection and Cursor objects. (http://www.python.org/dev/peps/pep-0249/)
No RDBMS specific code should be in here.
No Orchestra specific code.
No application (rodeo, roundup, genotator, etc.) specific code.

Things this module does not do:
  Get connections, since that is specific to the database server being used (MySQL, Oracle, etc.)
  Generate SQL, since that can be database-specific too. (some exceptions might apply.)
'''

import contextlib
import logging


class Reuser(object):
    '''
    An instance is a callable object that returns an open connection.  Multiple
    calls will return the same connection, if the connection is still "live".
    It will open a new connection if no connection is open yet or the existing
    connection is closed (or otherwise fails to be pinged.)
    This object is meant to handle two common scenarios with one API.
    * Opening and closing a connection very rapidly can cause a database to
      balk.  Reuser allows a process to open one connection and reuse it.
    * A connection kept open a long time can be closed by the database. 
      Reuser will open a new connection when the existing connection can
      not be reused.

    '''
    def __init__(self, open_conn):
        '''
        open_conn: function that returns an open connection
        '''
        self.open_conn = open_conn
        self.conn = None

    def __call__(self):
        '''
        returns: an open db connection, opening one if necessary.
        '''
        if not self._ping():
            self.conn = self.open_conn()
        return self.conn

    def _ping(self):
        '''
        returns: True if there is an active connection.  False otherwise.
        '''
        if self.conn:
            try:
                selectSQL(self.conn, 'SELECT 1')
                return True
            except Exception as e: 
                # OperationalError 2006 happens when the db connection times
                # out.
                if not e.args or e.args[0] != 2006:
                    # only log non-2006 execptions
                    logging.exception('Exception encountered when pinging connection in Reuser._ping.')
                return False
        else:
            return False


@contextlib.contextmanager
def doTransaction(conn, start=True, startSQL='START TRANSACTION'):
    '''
    wrap a connection in a transaction.  starts a transaction, yields the conn, and then if an exception occurs, calls rollback().  otherwise calls commit().
    start: if True, executes 'START TRANSACTION' sql before yielding conn.  Useful for connections that are autocommit by default.
    startSQL: override if 'START TRANSACTION' does not work for your db server.
    '''
    try:
        if start:
            executeSQL(conn, startSQL)
        yield conn
    except:
        conn.rollback()
        raise
    else:
        conn.commit()


@contextlib.contextmanager
def doCursor(conn):
    '''
    create and yield a cursor, closing it when done.
    '''
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
    

def selectSQL(conn, sql, args=None, asdict=False):
    '''
    Returns a sequence of rows, each of which is a tuple or a dict.  For example
    [(1, 'Jane'), (2, 'Joe')]

    sql: a select statement, e.g. 'SELECT id, name FROM people LIMIT %s'
    args: if sql has parameters defined with either %s or %(key)s then args
    should be a either list or dict of parameter values respectively.  e.g.
    [10]
    asdict: First, see notes below, as YMMV when getting results as a dict.
    If asdict is True, a sequence of dicts is returned, not a sequence of
    tuples.  Each dict maps the field name from cursor.description to the row
    field value.  e.g. [{'id': 1, 'name': 'Jane'}, {'id': 2, 'name': 'Joe'}]

    Notes from DB API 2.0, http://www.python.org/dev/peps/pep-0249/:

        Cursor Objects should respond to the following methods and
            attributes:

                .description 

                    This read-only attribute is a sequence of 7-item
                    sequences.  

                    Each of these sequences contains information describing
                    one result column: 

                    (name, 
                    type_code, 
                    display_size,
                    internal_size, 
                    precision, 
                    scale, 
                    null_ok)

                    The first two items (name and type_code) are mandatory,
                    the other five are optional and are set to None if no
                    meaningful values can be provided.

        Question: 

            How can I construct a dictionary out of the tuples returned by
            .fetch*():

        Answer:

            There are several existing tools available which provide
            helpers for this task. Most of them use the approach of using
            the column names defined in the cursor attribute .description
            as basis for the keys in the row dictionary.

            Note that the reason for not extending the DB API specification
            to also support dictionary return values for the .fetch*()
            methods is that this approach has several drawbacks:

            * Some databases don't support case-sensitive column names or
                auto-convert them to all lowercase or all uppercase
                characters.

            * Columns in the result set which are generated by the query
                (e.g.  using SQL functions) don't map to table column names
                and databases usually generate names for these columns in a
                very database specific way.

            As a result, accessing the columns through dictionary keys
            varies between databases and makes writing portable code
            impossible.
    '''
    with doCursor(conn) as cursor:
        cursor.execute(sql, args)
        tuples = cursor.fetchall()
        if asdict:
            names = [column[0] for column in cursor.description]
            numcols = len(names)
            dicts = [dict((names[i], row[i]) for i in range(numcols)) for row in tuples]
            return dicts
        else:
            return tuples


def insertSQL(conn, sql, args=None):
    '''
    args: if sql has parameters defined with either %s or %(key)s then args should be a either list or dict of parameter
    values respectively.
    returns the insert id
    '''
    with doCursor(conn) as cursor:
        cursor.execute(sql, args)
        id = conn.insert_id()
        return id


def updateSQL(conn, sql, args=None):
    '''
    args: if sql has parameters defined with either %s or %(key)s then args should be a either list or dict of parameter
    values respectively.
    returns the number of rows affected by the sql statement
    '''
    with doCursor(conn) as cursor:
        numRowsAffected = cursor.execute(sql, args)
        return numRowsAffected


def executeSQL(conn, sql, args=None):
    '''
    args: if sql has parameters defined with either %s or %(key)s then args should be a either list or dict of parameter
    values respectively.
    executes sql statement.  useful for executing statements like CREATE TABLE or RENAME TABLE,
    which do not have an result like insert id or a rowset.
    returns: the number of rows affected by the sql statement if any.
    '''
    with doCursor(conn) as cursor:
        numRowsAffected = cursor.execute(sql, args)
        return numRowsAffected
    

def executeManySQL(conn, sql, args=None):
    '''
    args: list of groups of arguments.  if sql has parameters defined with either %s or %(key)s then groups should be a either lists or dicts of parameter
    values respectively.
    returns: not sure.  perhaps number of rows affected.
    '''
    with doCursor(conn) as cursor:
        retval = cursor.executemany(sql, args)
        return retval



# last line
