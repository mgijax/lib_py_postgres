# Module: pg_db.py      (Postgres db module)
# Purpose: to serve as a wrapper over a dbManager (which itself handles both
#       MySQL and Postgres interaction) in a manner analagous to our existing
#       db.py module (used for Sybase interaction)
#
# 01/09/206  lec
#       TR12069/removing obsolete translation functions/variables
#
# 11/21/2016 - TR12069 setAutoTranslate* to False
#
# 10/22/2014    ks/lec
#       - TR11750/postgres
#
# 04/09/2012   sc
#       - returnAsSybase, getReturnAsSybase(), setReturnAsSybase(Boolean)
#
# 04/09/2012    lec
#       - autoTranslate_be; translate_be(); setAutoTranslateBE, setTrace
#

import os
import sys
import types
import re
import time
import dbManager

# unused globals, but kept for compatability

trace = 0
sql_client_msg = None
sql_server_msg = None
sql_client_msg_threshold = 1
sql_server_msg_threshold = 1
sql_cmd_buffer = None
sql_log_function = None
sql_log_fd = sys.stderr

# named exceptions, kept for compatability

connection_exc = 'pg_db.connection_exc'
error = 'pg_db.error'

# connection info

# this will set the default password for the default user (mgd_dbo)
try:
        password = open(os.environ['PG_1LINE_PASSFILE'], 'r').readline().strip()
except:
        password = 'mgdpub'

# The default should be to always use one connection
onlyOneConnection = 1

targetDatabaseType = 'postgres'
sharedDbManager = None

returnAsSybase = True

commandLogFile = None

###--- Functions ---###

def setTrace(on = True):
        global trace
        trace = on
        return

def __date():
        return time.strftime('%c', time.localtime(time.time()))

def __getDbManager():
        if targetDatabaseType == 'postgres':
                dbmType = dbManager.postgresManager
        elif targetDatabaseType == 'mysql':
                dbmType = dbManager.mysqlManager

        dbm = dbmType(server, database, user, password)
        dbm.setReturnAsSybase(returnAsSybase)
        return dbm

def  executeCopyFrom(
        file,           # file-like object to read data from.
                        #   It must have read() AND readline() methods.
        table,          # name of the table to copy data into.
        sep='\t',       # columns separator expected in the file.
                        #   Defaults to a tab.
        #null='\\\N',    # textual representation of NULL in the file.
        null=r"\N",
                        #   The default is the two characters string \N.
        size=8192,      # size of the buffer used to read from the file.
        columns=None):  # iterable with name of the columns to import. The
                        #  length and types should match the content of the
                        #  file to read. If not specified, it is assumed
                        #  that the entire table matches the file structure.
        global sharedDbManager

        if not onlyOneConnection:
                dbm = __getDbManager()
        else:
                if not sharedDbManager:
                        sharedDbManager = __getDbManager()
                dbm = sharedDbManager
        dbm.executeCopyFrom(file, table, sep, null, size, columns)
        return

def sqlLogCGI (**kw):
        sqlLogAll(kw)
        return

def sqlLog (**kw):
        sqlLogAll(kw)
        return

def sqlLogAll (**kw):
        msg = [ 'Date/time: %s' % __date(),
                'Server: %s' % server,
                'Database: %s' % database,
                'User: %s' % user,
                ]
        keys = list(kw.keys())
        keys.sort()
        for key in keys:
                if kw[key] == list:
                        i = 0
                        for item in kw[key]:
                                msg.append ('%s[%d] : %s' % (key, i,
                                        str(item)) )
                                i = i + 1
                else:
                        msg.append ('%s : %s' % (key, str(kw[key])))

        if sql_log_fd:
                sql_log_fd.write('\n'.join(msg))
                sql_log_fd.write('\n')
        return

def logCommand (cmd):
        if commandLogFile:
                commandLogFile.write(cmd)
                commandLogFile.write('\n')
                commandLogFile.flush()
        return

# setters

def set_commandLogFile(s):
        global commandLogFile
        commandLogFile = open(s, 'w')
        return

def set_sqlUser(s):
        global user
        user = s
        return

def set_sqlPassword(s):
        global password
        password = s
        return

def set_sqlPasswordFromFile(f):
        # find and set the password for the current user, as found in the file
        # with the name specified by 'f'.  Note that the password is expected
        # to be the entire contents of the file's first line.

        try:
                fp = open(f, 'r')
                s = fp.readline().rstrip()
                fp.close()
                set_sqlPassword(s)
        except:
                raise error('Cannot read from %s' % f)
        return

def set_sqlPasswordFromPgpass (filename):
        # find and set the password for the current user, as found in the
        # given 'filename' (a file formatted as a Postgres .pgpass file)

        try:
                foundUser = False

                fp = open (filename, 'r')
                line = fp.readline().strip()

                while line and not foundUser:
                        pieces = line.split(':')

                        if len(pieces) == 5:
                                if pieces[3] == user:
                                        foundUser = True
                                        set_sqlPassword(pieces[4])

                        line = fp.readline().strip()

                fp.close()

                if not foundUser:
                    raise error('Could not find user (%s) in %s' % (user, filename))
        except:
                #raise error, 'Cannot read from %s' % filename

                # This should no longer be an error state, as some servers do
                # not have the pgdbutilities product installed.  In case of
                # error, we just silently ignore the failure here and let any
                # calling scripts fail on their own, as needed.

                pass

        return

def set_sqlServer(s):
        global server
        server = s
        return

def set_sqlDatabase(s):
        global database
        database = s
        return

def set_sqlLogin (user, password, server, database):
        old = (user, password, server, database)

        set_sqlUser(user)
        set_sqlPassword(password)
        set_sqlServer(server)
        set_sqlDatabase(database)

        return old

def set_targetDatabaseType (t):
        global targetDatabaseType
        targetDatabaseType = t.lower()
        if targetDatabaseType not in [ 'postgres', 'mysql' ]:
                raise error('Unknown targetDatabaseType: %s' % t)
        return

def setReturnAsSybase (flag         # boolean; True for Sybase-style,
                                    # ...False if not
    ):
    # Purpose: configure to either return Sybase-style
    #       results (True) or not (False)

    global returnAsSybase
    returnAsSybase = flag
    return

def set_sqlLogFunction(f):
        """ 
        stub to support backward compatibility
        """
        pass

def useOneConnection (onlyOne = 0):
        global onlyOneConnection
        onlyOneConnection = onlyOne
        return

# getters

def get_commandLogFile():
        return commandLogFile

def get_sqlUser():
        return user

def get_sqlPassword():
        return password

def get_sqlServer():
        return server

def get_sqlDatabase():
        return database

def get_targetDatabaseType (t):
        return targetDatabaseType

def getReturnAsSybase ():
    # Purpose: return the flag for whether our results are Sybase-style
    #       (True) or not (False)
    # Returns: boolean

    return returnAsSybase

# main method

def sql (command, parser = 'auto', **kw):
        # return type is dependent on 'parser' and on the value of
        # the global returnAsSybase
        global sharedDbManager

        if not onlyOneConnection:
                dbm = __getDbManager()
        else:
                if not sharedDbManager:
                        sharedDbManager = __getDbManager()
                dbm = sharedDbManager

        if 'row_count' in kw:
                rowCount = kw['row_count']
                if rowCount == 0:
                        rowCount = None
        else:
                rowCount = None

        singleCommand = False
        autoParser = (parser == 'auto')

        if type(command) != list:
                command = [ command ]
                singleCommand = True

        if type(parser) != list:
                parser = [ parser ] * len(command)

        if rowCount:
                if type(rowCount) == list:
                        pass
                else:
                        if type(rowCount) != int:
                                rowCount = int(rowType)
                        rowCount = [ rowCount ] * len(command)

        if len(command) != len(parser):
                raise error('Mismatching counts in command and parser')
        elif rowCount and (len(command) != len(rowCount)):
                raise error('Mismatching counts in command and rowCount')

        resultSets = []

        i = 0
        while (i < len(command)):
                cmd = command[i]
                psr = parser[i]

                selectPos = cmd.find('select')
                if selectPos < 0:
                        selectPos = cmd.find('SELECT')

                # apply row limits for select statements
                if rowCount and rowCount[i] and (selectPos >= 0):
                        cmd = cmd + ' limit %d' % rowCount[i]

                if trace:
                        sys.stderr.write ('%s\n' % str(cmd))
                        #sys.stderr.write ('pg parser: %s\n' % str(psr))

                logCommand(cmd)
                results = dbm.execute(cmd)

                if psr is None:
                        pass
                elif psr != 'auto':
                        for row in results:
                                psr(row)
                else:
                        resultSets.append (results)
                i = i + 1

        if not autoParser:
                return None
        if singleCommand:
                return results
                #return resultSets[0]
        return resultSets


def bcp(inputFileName,
        table,
        delimiter='\\t',
        schema='mgd',
        disableTriggers=False):
    """
    BCP inputFile into table
        using delimiter for column seperation

        NOTE: disabling triggers locks entire table for
                other connections during transaction
                until commit or rollback
    """

    if schema:
        table = '%s.%s' % (schema, table)

    bcpCommand = "copy %s from STDIN with null as '' delimiter as E'%s' " % \
        (table, delimiter)


    # open file for STDIN read
    inputFile = open(inputFileName, 'r')

    
    if disableTriggers:
        disableTrigger = "ALTER TABLE %s DISABLE TRIGGER USER" % \
            (table)
        sql(disableTrigger, None)
        
    try:
        cur = sharedDbManager.sharedConnection.cursor()
        cur.copy_expert(bcpCommand, inputFile)
    finally:
        inputFile.close()

    if disableTriggers:
        enableTrigger = "ALTER TABLE %s ENABLE TRIGGER USER" % \
            (table)
        sql(enableTrigger, None)


# record of 'create index' commands by table
#       logged after every disableIndices() call
#       used by reenableIndices()
INDEX_CREATE_COMMANDS = {}

def disableIndices(table):
    """
    Disable all indices on table
        to be restored by reenableIndices()
    """
    global INDEX_CREATE_COMMANDS
    table = table.lower()
    
    # find current indices
    results = sql("""select indexname, indexdef 
        from pg_indexes 
        where tablename='%s'
    """, "auto")

    INDEX_CREATE_COMMANDS[table] = []

    for result in results:
        INDEX_CREATE_COMMANDS[table].append(result['indexdef'])
        sql("DROP INDEX %s" % results['indexname'], None)
    


def reenableIndices(table):
    """
    re-enable indices on table
        only after a call to disableIndices()
        on the same table
    """
    table = table.lower()

    if table not in INDEX_CREATE_COMMANDS:
        raise Exception("""
            Cannot re-enable indices for %s. No record of disabled indices
        """ % table)

    for createCommand in INDEX_CREATE_COMMANDS[table]:
        sql(createCommand, None)


def commit():
        if sharedDbManager:
                sharedDbManager.commit()
        return

###--- initialization ---###

# list of (environment variable name, function to call with its value)
# so we can easily pick up default settings from the environment.  Note that
# later options override earlier ones, if multiple environment variables are
# available.  So, put preferred ones last.  Also note that settings for the
# user should come before those for a password file.
environSettings = [
        ('PG_DBSERVER', set_sqlServer),
        ('PG_DBNAME', set_sqlDatabase),
        ('PG_DBUSER', set_sqlUser),
        ('PG_DBPASSWORDFILE', set_sqlPasswordFromPgpass),
        ('PGPASSFILE', set_sqlPasswordFromPgpass),
        ]

for (name, fn) in environSettings:
        if name in os.environ:
                fn(os.environ[name])

