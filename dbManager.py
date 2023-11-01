# Name: dbManager.py
# Purpose: to provide a simple, consistent, convenient mechanism for working with Postgres database connections
#
# see HISTORY for more details
#

import os
import traceback
import sys
import time
import datetime

###--- Globals ---###

LOADED_POSTGRES_DRIVER = False  # have we loaded the Postgres python module?
POSTGRES = 'Postgres'           # constant; identifies type of dbManager

class DbManagerError(Exception):
        """
        Custom error class
        """
        pass

try:
        import psycopg2
        LOADED_POSTGRES_DRIVER = True
except:
        pass

if LOADED_POSTGRES_DRIVER:
        #
        # For software compatibility we convert all Decimals to float Psycopg2 sets Numerics as Decimal by default
        #
        import psycopg2.extensions

        def decimalToFloat(value, curs):
                if value != None:
                        return float(value)
                return None

        DEC2FLOAT = psycopg2.extensions.new_type(psycopg2._psycopg.DECIMAL.values, 'DEC2FLOAT', decimalToFloat)
        psycopg2.extensions.register_type(DEC2FLOAT)

# For Postgres, we do not fail immediately if we cannot get a connection due to the server having too many connections.  
# We want to wait and try again a few times before giving up.  
# These parameters are specifically for use by the postgresManager:

MAX_ATTEMPTS = 10       # integer; number of attempted connections before fail
INITIAL_DELAY = 0.1     # integer; initial delay between attempts (in seconds)

# Note that the delay is doubled after each failed attempt to connect (after the first).  
# This results in delays of: 0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8, and 25.6 seconds for a total of 51.1 seconds 
# before we give up entirely.  

###--- Classes ---###

# Is: a manager for a database connection
# Has: connection parameters and, optionally, a shared connection to the database
# Does: can use the 'dbManager' to either give you back a connection to manage yourself, 
#       or can use its execute() method to use the dbManager's shared connection (and let it manage the connection for you).
#       Any changes made using execute() should either be confirmed (using commit()) or rolled back (using rollback()).
class dbManager:
    # Purpose: constructor
    # Returns: nothing
    # Assumes: nothing
    # Modifies: reads the 'passwordFile' from the file system, if that parameter is specified
    # Throws: propagates any exceptions raised if we cannot read the specified password file
    # Notes: must specify either a 'password' or a 'passwordFile'
    def __init__ (self,
        host,                   # string; host for database server
        database,               # string; name of database w/in the server
        user,                   # string; username for database
        password=None,          # string; password to go with 'username'
        passwordFile=None       # string; path to password file
        ):

        self.dbType = None      # must be filled in by subclass's setDbType()
        self._setDbType()       # ...method
        self.sharedConnection = None    # shared Connection object

        # should we return results as a list of dictionaries (default is False)
        self.returnAsMGI = False

        # connection parameters
        self.host = host
        self.database = database
        self.user = user
        if password:
                self.password = password
        elif passwordFile:
                self.password = _readPasswordFile (passwordFile)
        else:
                raise DbManagerError('Could not initialize; no password specified')

        return

    # Purpose: get a new connection to the database
    # Returns: Connection object
    # Assumes: nothing
    # Modifies: opens a database connection
    # Throws: Exception if we cannot make a database connection
    def getConnection (self):

        self.__checkDbType()

        try:    
                connection = self._getConnection()
        except:
                (excType, excValue, excTraceback) = sys.exc_info()
                traceback.print_exception (excType, excValue, excTraceback)
                raise DbManagerError ('Cannot get connection to %s:%s as %s' % (self.host, self.database, self.user))

        return connection

    # Purpose: execute the given 'cmd' on the shared connection
    # Returns: natively, a 2-item tuple:
    #       (list of column names, list of lists -- each inner list of a
    #       list of values for the columns for that row)
    #       or, if operating in 'return as MGI' mode, returns:
    #       list of dictionaries, each of which has fieldnames as keys
    #       with each mapped to its value for that row
    # Assumes: we have the necessary permissions to execute the database statement
    # Modifies: could alter database structure or contents, depending on what 'cmd' is
    # Throws: Exception if we cannot get a connection to use or if we cannot execute the given 'cmd'
    def execute (self,
        cmd             # string; SQL statement to execute on this dbManager's shared connection
        ):

        # instantiate a connection, if we have not yet done so
        if not self.sharedConnection:
                self.sharedConnection = self.getConnection()

        # get a cursor for executing the desired SQL statement
        cursor = self.sharedConnection.cursor()
        try:
                cursor.execute (cmd)
        except Exception as e:
                self.sharedConnection.rollback()
                cursor.close()
                raise DbManagerError ('Command failed (%s) Error: %s' % (cmd, str(e.args)))

        # convert column names in cursor.description list into a simple list of field names
        columns = []
        if cursor.description:
                for tpl in cursor.description:
                        columns.append (tpl[0])

        # if we did not find any columns, then there are no rows to retrieve
        if not columns:
                cursor.close()
                return None, None

        # retrieve the data rows and close the cursor
        rows = cursor.fetchall()
        cursor.close()

        # if converting return value to mimic db.sql() function for MGI, then convert the columns and rows
        if self.returnAsMGI:
                return _asMGI ( (columns, rows) )

        return columns, rows

    # Purpose: examine the flag for whether our results are MGI-style (True) or not (False)
    # Returns: boolean
    # Assumes: nothing
    # Modifies: nothing
    # Throws: nothing
    def getReturnAsMGI (self):
        return self.returnAsMGI

    # Purpose: configure this dbManager to either return MGI-style results (True) or not (False)
    # Returns: nothing
    # Assumes: nothing
    # Modifies: nothing
    # Throws: nothing
    def setReturnAsMGI (self, 
                flag # boolean; True for MGI-style, False if not
        ):
        self.returnAsMGI = flag
        return

    # Purpose: issue a 'commit' command on the shared connection, if one is open
    # Returns: nothing
    # Assumes: nothing
    # Modifies: commits any outstanding changes to the database for the current shared connection
    # Throws: nothing
    def commit (self):
        if self.sharedConnection:
                self.sharedConnection.commit()
        return

    # Purpose: issue a 'rollback' command on the shared connection, if one is open
    # Returns: nothing
    # Assumes: nothing
    # Modifies: rolls back any outstanding changes to the database for the current shared connection
    # Throws: nothing
    def rollback (self):
        if self.sharedConnection:
                self.sharedConnection.rollback()
                self.sharedConnection.close()
        self.sharedConnection = None
        return

    # Purpose: check that our dbManager knows what type of database it
    #       should comminicate with; this is used internally by the
    #       dbManager class to ensure that 'self' is of a subclass of
    #       dbManager, rather than the parent class itself
    # Returns: nothing
    # Assumes: nothing
    # Modifies: nothing
    # Throws: Error if it does not know the type of database
    def __checkDbType (self):
        if not self.dbType:
                raise DbManagerError('Cannot instantiate dbManager class directly; must use a subclass')
        return

    # Purpose: to get a database connection; this is an internal method that must be implemented in subclasses of dbManager
    # Returns: a database connection
    # Assumes: nothing
    # Modifies: nothing
    # Throws: Error if this method was not re-implemented in 'self'
    def _getConnection (self):
        raise DbManagerError( 'Must implement _getConnection() in a subclass')

    # Purpose: to set the database type for this dbManager; this is an internal method that must be implemented in subclasses of dbManager
    # Returns: nothing
    # Assumes: nothing
    # Modifies: nothing
    # Throws: Error if this method was not re-implemented in 'self'
    def _setDbType (self):
        raise DbManagerError('Must implement _setDbType() in a subclass')

# Is: a dbManager that knows how to interact with Postgres
# Has: see dbManager
# Does: see dbManager
class postgresManager (dbManager):

    # Purpose: to set this dbManager's database type to be POSTGRES
    # Other: see dbManager._setDbType() for other comments
    def _setDbType (self):
        self.dbType = POSTGRES
        return

    # Purpose: to get a connection to a Postgres database
    # Returns: connection object
    # Assumes: nothing
    # Modifies: nothing
    # Throws: propagates certain exceptions from psycopg2.connect() method
    def _getConnection (self):

        if not LOADED_POSTGRES_DRIVER:
                raise DbManagerError('Cannot get connection; psycopg2 driver was not loaded')

        conn = None             # connection to be returned
        attempts = 0            # number of attempts to get connection so far
        delay = INITIAL_DELAY   # current delay (in seconds) before next retry

        while not conn:
                try:
                        attempts = attempts + 1
                        conn = psycopg2.connect (host=self.host, user=self.user, password=self.password, database=self.database)
                except:
                        (excType, excValue, excTraceback) = sys.exc_info()

                        # specific errors to look for from postgres...
                        #       1. bad password or username
                        #       2. bad database
                        #       3. bad server
                        # These ones are fatal, so bail out.  
                        # If none of these are found, assume too many connections and wait to try again a few times.

                        exc = str(excValue)
                        msg = None

                        if exc.find('password authentication failed') >= 0:
                                msg = 'Unknown user (%s) or password on %s' % (self.user, self.host)

                        elif (exc.find('database "') >= 0) and (exc.find('"does not exist') >= 0):
                                msg= 'Unknown database (%s) on %s' % ( self.database, self.host)
                        
                        elif exc.find('could not translate host') >= 0:
                                msg = 'Unknown host %s' % self.host
                        
                        elif attempts >= MAX_ATTEMPTS:
                                msg = 'Failed to get connection for %s:%s as %s; giving up (attempt %d)' % (self.host, self.database, self.user, attempts)

                        if msg:
                                traceback.print_exception (excType, excValue, excTraceback)
                                sys.stderr.write('dbManager: %s\n' % msg)
                                raise DbManagerError (msg)
                        else:
                                sys.stderr.write ('dbManager: Failed to get connection for %s:%s as %s; waiting to retry (attempt %d)\n' % (
                                        self.host, self.database, self.user, attempts) )

                        time.sleep(delay)
                        delay = delay * 2.0     # double delay for next time

        return conn

    def executeCopyFrom(
            self, 
            file,           # file-like object to read data from.  It must have read() AND readline() methods.
            table,          # name of the table to copy data into.
            sep='\t',       # columns separator expected in the file.  Defaults to a tab.
            null=r"\N",     # The default is the two characters string \N.
            size=8192,      # size of the buffer used to read from the file.
            columns=None    # iterable with name of the columns to import.
                            #  The length and types should match the content of the
                            #  file to read. If not specified, it is assumed 
                            #  that the entire table matches the file structure.
            ):
        cursor = self.sharedConnection.cursor()
        try:
            cursor.copy_from(file, table, sep, null, size, columns)
        except Exception as e:
                self.sharedConnection.rollback()
                cursor.close()
                raise DbManagerError ('Command failed (%s) Error: %s' % (cmd, str(e.args)))
        return

# MGI representation of a Result/Row returned by Postgres
class MGIDict:

        def __init__ (self, d = {}):
                self.myDict = d
                return

        def keys (self):
                return list(self.myDict.keys())

        def items (self):
                return list(self.myDict.items())

        def has_key (self, key):
                try:
                        self.resolve(key)
                        return True
                except:
                        return False

        def __getitem__ (self, key):
                return self.myDict[self.resolve(key)]

        def __setitem__ (self, key, value):
                self.myDict[key.lower()] = value
                return

        def str (self):
                return str(self.myDict)

        def __repr__ (self):
                return str(self.myDict)

        def __len__ (self):
                return len(self.myDict)

        def resolve (self, key):
                if key in self.myDict:
                        return key

                lowerKey = key.lower()
                if lowerKey in self.myDict:
                        return lowerKey

                if lowerKey == 'offset':
                        return self.resolve('cmOffset')

                raise DbManagerError( 'Unknown key (%s) from: %s' % (key, list(self.myDict.keys()) ) )

###--- Functions ---###

# Purpose: retrieve the password contained in the file identified by its given path
# Returns: string; the password from the file
# Assumes: nothing
# Modifies: nothing
# Throws: Exception if we cannot read the password file
def _readPasswordFile (
        file            # string; path to file containing a password
        ):

        if not os.path.exists(file):
                raise DbManagerError( 'Unknown password file: %s' % file)
        try:
                fp = open(file, 'r')
                password = fp.readline().trim()
                fp.close()
        except:
                raise DbManagerError( 'Cannot read password file: %s' % file)

        return password

# Purpose: convert the dbManager-format query returns into a MGI-style list of dictionaries, 
#       as would be returned by db.sql() for a single SQL statement
# Returns: list of dictionaries.  Each dictionary is for one row of data, with fieldnames as keys and field values as values.
# Assumes: nothing
# Modifies: nothing
# Throws: nothing
def _asMGI (
        columnsAndRows          # tuple, as returned by execute()
        ):

        columns, rows = columnsAndRows
        mgiRows = []
        for row in rows:
                mgiRow = {}

                i = 0
                for col in columns:
                        if type(row[i]) == datetime.datetime:
                                mgiRow[col] = str(row[i])
                        else:
                                mgiRow[col] = row[i]
                        i = i + 1

                mgiRows.append (MGIDict(mgiRow))

        return mgiRows

