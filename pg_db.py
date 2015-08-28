# Module: pg_db.py	(Postgres db module)
# Purpose: to serve as a wrapper over a dbManager (which itself handles both
#	MySQL and Postgres interaction) in a manner analagous to our existing
#	db.py module (used for Sybase interaction)
#
# 10/22/2014	ks/lec
#	- TR11750/postgres
#
# 04/09/2012   sc
#	- returnAsSybase, getReturnAsSybase(), setReturnAsSybase(Boolean)
#
# 04/09/2012	lec
#	- autoTranslate_be; translate_be(); setAutoTranslateBE, setTrace
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

# catch both != and = WHERE clauses
equalClause = re.compile ("([\s(])([A-Za-z_\.0-9]+) *(!?=) *'([^']*)'")

# catch both 'in' and 'not in' WHERE clauses
inClause = re.compile ("([\s(])([A-Za-z_\.0-9]+) *(not)? *(in) *\(('[^)]+)\)",
	re.IGNORECASE)

# catch "x = y" (for renaming) in the select clause (We catch the easy case
# here, not worrying about strings with embedded spaces and such.)
renameClause = re.compile ("([\s])([A-Za-z_0-9]+) *= *([^\s,]+)")
		
def translate (cmd):

	cmd1 = cmd.replace ('"', "'")

	# convert exec statement for stored procedures
	cmdlower = cmd1.lower()
	if 'exec ' in cmdlower:
		execIdx = cmdlower.find('exec')
		sp_args = cmd1[(execIdx+4):].lstrip().split(' ',1)
		sp = sp_args[0]
		args = sp_args[1]
		cmd1 = "select * from %s (%s);" % (sp, args)

	#
	# GO regex conversions
	# 
	cmd1 = cmd1.replace ('not like \'[A-Z][0-9][0-9][0-9][0-9][0-9]\'',
		'!~ \'^[a-zA-Z][0-9][0-9][0-9][0-9][0-9]$\'') 
	cmd1 = cmd1.replace ('not like \'[A-Z][A-Z][0-9][0-9][0-9][0-9][0-9][0-9]\'',
		'!~ \'^[a-zA-Z][a-zA-Z][0-9][0-9][0-9][0-9][0-9][0-9]$\'') 


	# handle date conversions
	cmd1 = cmd1.replace ('convert(char(10),', 'to_char(')
	cmd1 = cmd1.replace (', 101)', ', \'MM/dd/yyyy\')')
	cmd1 = cmd1.replace ('dateadd(day, 1, g.cdate)', '(g.cdate + interval \'1 day\')')

	cmd1 = cmd1.replace ('.offset', '.cmOffset')
	cmd1 = cmd1.replace (' like ', ' ilike ')
	cmd1 = cmd1.replace (' LIKE ', ' ILIKE ')
	cmd1 = cmd1.replace (' null ', ' NULL ')
	cmd1 = cmd1.replace ('!= NULL', 'is not null')

	# do not change the "= NULL" to "is" if the "= NULL" exists within an update statement
	if 'update ' not in cmd1.lower():
		cmd1 = cmd1.replace ('= NULL', 'is null')

	# We want to ensure that any "equals" comparisons in the WHERE section
	# are done on a case insensitive basis, to match Sybase's behavior.

	wherePos = cmd1.find('where')
	if wherePos < 0:
		wherePos = cmd1.find('WHERE')

	if wherePos >= 0:
		# convert any 'equals' or 'not equals' comparisons

		cmd2 = ''
		last = 0
		match = equalClause.search(cmd1, wherePos)
		while match:
			start, stop = match.regs[0]
			cmd2 = cmd2 + cmd1[last:start]
			cmd2 = cmd2 + "%slower(%s) %s '%s'" % (
				match.group(1), match.group(2),
				match.group(3), match.group(4).lower() )
			last = stop
			match = equalClause.search(cmd1, last)

		cmd2 = cmd2 + cmd1[last:] 

		# convert any 'in' or 'not in' comparisons

		cmd3 = ''
		last = 0
		match = inClause.search(cmd2, wherePos)
		while match:
			start, stop = match.regs[0]
			cmd3 = cmd3 + cmd2[last:start]

			if match.group(3) == None:
				op = 'in'
			else:
				op = 'not in'

			cmd3 = cmd3 + "%slower(%s) %s (%s)" % (
				match.group(1), match.group(2), op,
				match.group(5).lower())
			last = stop
			match = inClause.search (cmd2, last)

		cmd3 = cmd3 + cmd2[last:]
	else:
		cmd3 = cmd1

	# convert any "x = y" notation in the SELECT section to be "y as x",
	# but leave 'update' statements alone

	updatePos = cmd3.find('update')
	if updatePos < 0:
		updatePos = cmd3.find('UPDATE')

	if (updatePos >= 0):
		# if a 'select' appears before an 'update', then we assume
		# that the 'update' is just part of a WHERE clause and go
		# ahead.  if the 'update' is first, then return as-is.

		selectPos = cmd3.find('select')
		if selectPos < 0:
			selectPos = cmd3.find('SELECT')

		if (selectPos < 0) or (selectPos > updatePos):
			return cmd3

	cmd4 = ''
	last = 0
	fromPos = cmd3.find('from')
	if fromPos < 0:
		fromPos = cmd3.find('FROM')
	if fromPos < 0:
		return cmd3
	match = renameClause.search(cmd3)
	while match and (match.regs[0][1] < fromPos):
		start, stop = match.regs[0]
		cmd4 = cmd4 + cmd3[last:start]
		cmd4 = cmd4 + match.group(1) + match.group(3) + ' as ' + \
			match.group(2)
		last = stop
		match = renameClause.search(cmd3, last)
	cmd4 = cmd4 + cmd3[last:]

	cmd1 = cmd4

	return cmd1

#
# back-end-specific translations
#
def translate_be (cmd):


	#
	# temporary tables
	# order is important!
	#

	# make sure the 'insert into #' is fixed first
	cmd1 = cmd.replace ('insert into #', 'insert into ')

	# any remaining 'into #' is expected to the the initial creation of the temp table
	cmd1 = cmd1.replace ('into #', 'INTO TEMPORARY TABLE ')
	cmd1 = cmd1.replace ('drop table', 'drop table if exists')

	# any remaining references to the temp table
	cmd1 = cmd1.replace ('#', '')

	# end: temporary tables

	# queries across different schemas
	# sybase: uses schema..table (imsr..Label)
	# postgres: uses schema.table (imsr.Label)
	cmd1 = cmd1.replace ('..', '.')

	#
	# copies from translate()
	#
	cmd1 = cmd1.replace ('offset', 'cmOffset')
	cmd1 = cmd1.replace (' like ', ' ilike ')
	cmd1 = cmd1.replace (' LIKE ', ' ILIKE ')
	cmd1 = cmd1.replace (' null ', ' NULL ')
	cmd1 = cmd1.replace ('!= NULL', 'is not null')

	# do not change the "= NULL" to "is" if the "= NULL" exists within an update statement
	if 'update ' not in cmd1.lower():
		cmd1 = cmd1.replace ('= NULL', 'is null')

	# end: translate()

	#
	# substring()
	#
	cmd1 = cmd1.replace ('substring', 'substr')
	# end: substring()

	#
	# convert()
	#
	cmd1 = cmd1.replace ('convert(varchar(10), g.modification_date, 112)', 'g.modification_date::DATE')
	cmd1 = cmd1.replace ('convert(char(10), t.completion_date, 112)', 't.completion_date::DATE')
	# handle date conversions
	cmd1 = cmd1.replace ('convert(char(10),', 'to_char(')
	cmd1 = cmd1.replace (', 101)', ', \'MM/dd/yyyy\')')

	# improve this; use regular expression
	items = ['c', 'scc', 'f', 'mcf', 'l']
	for x in items:
	    cmd1 = cmd1.replace ('convert(int, %s.startCoordinate)' % (x), \
			'cast(%s.startcoordinate as varchar)' % (x))
	    cmd1 = cmd1.replace ('convert(int, %s.endCoordinate)' % (x), \
			'cast(%s.endcoordinate as varchar)' % (x))

	cmd1 = cmd1.replace ('convert(int, sgt.pointCoordinate)', 'cast(sgt.pointCoordinate as varchar)')
	cmd1 = cmd1.replace ('convert(char(20), getdate(), 100)', 'current_date')
	cmd1 = cmd1.replace('convert(varchar(2), t.stage)', 't.stage::text')
	# end: convert()

	#
	# creation_date
	# modification_date
	# this set of converts are used primarly by the 'markerfeed' product
	# '%screation_date::DATE' % (x))
	#
	items = ['', 'a.', 'b.', 'e.', 'h.', 'm.', 'n.', 'p.', 'r.', 's.', 't.', 'va.']
	for x in items:
	    cmd1 = cmd1.replace ('convert(char(20), %screation_date, 100)' % (x), \
			'to_char(%screation_date, \'Mon DD YYYY HH:MMPM\')' % (x))
	    cmd1 = cmd1.replace ('convert(char(20), %smodification_date, 100)' % (x), \
			'to_char(%smodification_date, \'Mon DD YYYY HH:MMPM\')' % (x))
	# end: creation_date/modification_date

	#
	# datepart(year, ...) -> date_part('year', ...)
	# datepart(month, ...) -> date_part('month', ...)
	#
	cmd1 = cmd1.replace("getdate()", "current_date")
	cmd1 = cmd1.replace("datepart(year,", "date_part('year',")
	cmd1 = cmd1.replace("datepart(month,", "date_part('month',")

	#
	# case
	#
	cmd1 = cmd1.replace ('str(o.cmOffset,10,2)', 'to_char(o.cmOffset, \'999.99\')')
	# end: case

	#
	# GXD_EarlyPapers.py str() usage
	#
	cmd1 = cmd1.replace ('str(br.year) + \' \' + ','br.year || \' \' || ')

	#
	# mrklabel.py str concat
	#
	cmd1 = cmd1.replace ('commonName + \'', 'commonName || \'')

	#
	# MRK_NoGO.py text conversions
	#
	cmd1 = cmd1.replace ('\'no \'','\'no \'::text')

	#
	# identity column for generating row numbers
	#
	cmd1 = cmd1.replace ('identity(10)', 'row_number() over()')

	#
	# create clustered index
	#
	cmd1 = cmd1.replace ('clustered index', 'index')

	#
	# replace sybase schema syntax
	#
	cmd1 = cmd1.replace ('..', '.')

	# convert delete from <table> from other tables syntax
	cmdlower = cmd1.lower()
	deleteIdx = cmdlower.find('delete')
	if deleteIdx >= 0:
		from1 = cmdlower.find('from', deleteIdx)
		from2 = cmd1.find('from', from1 + 1) 
		if from2 < 0:
		    from2 = cmd1.find('FROM', from1 + 1) 
		
		if from2 > deleteIdx and 'select' not in cmd1[:from2].lower():
			cmd1 = cmd1[:from2] + "USING" + cmd1[(from2 + 4):]

	#
	# 'E' as source
	# 'L' as source
	#
	cmd1 = cmd1.replace("'E' as source", "'E'::text as source")
	cmd1 = cmd1.replace("'L' as source", "'L'::text as source")

	return cmd1

def  executeCopyFrom(
	file,           # file-like object to read data from.
			#   It must have read() AND readline() methods.
	table,          # name of the table to copy data into.
	sep='\t',       # columns separator expected in the file.
			#   Defaults to a tab.
	null='\\\N',    # textual representation of NULL in the file.
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
	keys = kw.keys()
	keys.sort()
	for key in keys:
		if kw[key] == types.ListType:
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
		raise error, 'Cannot read from %s' % f
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
		    raise error, \
			'Could not find user (%s) in %s' % (user, filename)
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
		raise error, 'Unknown targetDatabaseType: %s' % t
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

	if kw.has_key('row_count'):
		rowCount = kw['row_count']
		if rowCount == 0:
			rowCount = None
	else:
		rowCount = None

	singleCommand = False
	autoParser = (parser == 'auto')

	if type(command) != types.ListType:
		command = [ command ]
		singleCommand = True

	if type(parser) != types.ListType:
		parser = [ parser ] * len(command)

	if rowCount:
		if type(rowCount) == types.ListType:
			pass
		else:
			if type(rowCount) != types.IntType:
				rowCount = int(rowType)
			rowCount = [ rowCount ] * len(command)

	if len(command) != len(parser):
		raise error, 'Mismatching counts in command and parser'
	elif rowCount and (len(command) != len(rowCount)):
		raise error, 'Mismatching counts in command and rowCount'

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

    bcpCommand = "copy %s.%s from STDIN with null as '' delimiter as E'%s' " % \
        (schema, table, delimiter)


    # open file for STDIN read
    inputFile = open(inputFileName, 'r')

    
    if disableTriggers:
        disableTrigger = "ALTER TABLE %s.%s DISABLE TRIGGER USER" % \
            (schema, table)
        sql(disableTrigger, None)
        
    try:
        cur = sharedDbManager.sharedConnection.cursor()
        cur.copy_expert(bcpCommand, inputFile)
    finally:
        inputFile.close()

    if disableTriggers:
        enableTrigger = "ALTER TABLE %s.%s ENABLE TRIGGER USER" % \
            (schema, table)
        sql(enableTrigger, None)


# record of 'create index' commands by table
# 	logged after every disableIndices() call
#	used by reenableIndices()
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
	if os.environ.has_key(name):
		fn(os.environ[name])

