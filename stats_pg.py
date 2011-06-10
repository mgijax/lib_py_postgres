# Module: stats_pg.py
# Author: jsb
# Purpose: This module provides Python scripts with easy access to data from
#	statistic-related tables introduced in MGI 4.0.  These tables include
#	MGI_Statistic, MGI_StatisticSql, and MGI_Measurement, as well as the
#	statistic-grouping functionality from MGI_Set and MGI_SetMember.
# Limitations: We intentionally leave some functionality out of this module.
#	For instance, we do not want to make it easy to delete a statistic
#	or measurement (to avoid accidental deletions), or to change the
#	abbreviation for a statistic (since it will be used in code in other
#	places).  If you want to do these things, we want you to be very aware
#	of it, and to require you to use SQL for them.  This is, quite simply,
#	"inconvenience by design".
# Notes: 
#	1. The first step in using this module is to give it a configured
#	sql() function.  For example, you would import the db module, use
#	db.set_sqlLogin() function to initialize it, then pass db.sql into
#	this module using stats_pg.setSqlFunction():
#		import db
#		import stats
#		db.set_sqlLogin ('username', 'password', 'server', 'database')
#		stats_pg.setSqlFunction (db.sql)
#	2. Any failure in this module should raise the exception stats_pg.ERROR
#	with an appropriate descriptive value.
#	3. This module provides three classes -- Measurement, Statistic, and
#	StatisticGroup.  You do not need to instantiate either of the first
#	two directly.  Convenience functions are provided to retrieve objects
#	of those classes. (see 4-5)
#	4. You can get back an individual Statistic object using convenience
#	functions by abbreviation (getStatistic), name (getStatisticByName),
#	or key (getStatisticByKey).
#	5. The getStatistics() function lets you get all Statistics (using no
#	parameter) or lets you get all the Statistics in a particular group
#	(using the group name as an optional parameter).
#	6. You can get a list of all group names using getAllGroups().
#	7. The getAllAbbrev() function gives you a list of all abbreviations
#	for Statistics.
#	8. You can use the recordMeasurement() function to add a new
#	measurement for a statistic.  This can be done by any db account, even
#	mgd_public.
#	9. A Statistic object provides a variety of get* methods which only
#	require read-only access.  It also provides set* methods which will
#	fail without mgd_dbo access.
#	10. You really only need to instantiate a StatisticGroup if you want
#	to change its members or their ordering.
#	11. You can use the measureAllHavingSql() function to add a new
#	measurement for each statistic which has SQL stored in the database.

import sys		# used to access info about exceptions
import types		# for type checking

#-----------------------------------------------------------------------------

###------------------------###
###--- Global variables ---###
###------------------------###

SQL = None		# function to use for db access (eg- db.sql)
ERROR = 'stats_pg.error'	# standard exception raised by this module

#-----------------------------------------------------------------------------

###---------------------------###
###--- Functions (private) ---###
###---------------------------###

def sql (
	cmds	# string (single SQL cmd) or list of strings (each a SQL cmd)
	):
	# Purpose: This is a wrapper over the db module's sql() function.  It
	#	accepts a single SQL command or a list of them, runs them with
	#	the 'auto' parser (see db.sql), and returns the results.
	# Returns: see db.sql
	# Assumes: global 'SQL' has been set appropriately by setSqlFunction()
	# Effects: sends 'cmds' to the database, which may include read-only
	#	queries or may include SQL 'insert' or 'update' statements
	# Throws: global 'ERROR' if problems occur, such as faulty 'cmds',
	#	improper database permissions, or lack of initialization

	if SQL == None:
		raise ERROR, 'stats_pg.py not initialized; call setSqlFunction'
	try:
		results = SQL(cmds, 'auto')
	except:
		excType, excValue = sys.exc_info()[:2]
		raise ERROR, \
			'Query failed: %s -- Exception: %s -- Value: %s' % (
				str(cmds), excType, excValue)
	return results

#-----------------------------------------------------------------------------

###---------------###
###--- Classes ---###
###---------------###

class Measurement:
	# Is: a single date/time-stamped measurement for a statistic
	# Has: a date/time and a numeric value
	# Does: provides accessor methods for the date/time and value

	#---------------------------------------------------------------------

	def __init__ (self,
		dateTime,	# string : date/timestamp for this measurement
		hasIntValue, 	# integer 0/1 : is 'intValue' the valid one?
		intValue, 	# integer or null : value for this measurement
		floatValue	# float or null : value for this measurement
		):
		# Purpose: constructor
		# Returns: nothing
		# Assumes: 1. either 'intValue' is non-null, or 'floatValue'
		#	is non-null, but not both.  2. the non-null one is the
		#	one indicated by 'hasIntValue'
		# Effects: constructs an object
		# Throws: nothing

		self.timestamp = dateTime
		self.hasInt = hasIntValue
		self.intValue = intValue
		self.floatValue = floatValue
		return

	#---------------------------------------------------------------------

	def getTimestamp (self):
		# Purpose: accessor method
		# Returns: string -- date/time this measurement was recorded
		# Assumes: nothing
		# Effects: nothing
		# Throws: nothing

		return self.timestamp

	#---------------------------------------------------------------------

	def hasIntValue (self):
		# Purpose: accessor method
		# Returns: integer (0/1) -- indicates whether this measurement
		#	is integer-valued (1) or not (0)
		# Assumes: nothing
		# Effects: nothing
		# Throws: nothing

		return self.hasInt

	#---------------------------------------------------------------------

	def getIntValue (self):
		# Purpose: accessor method
		# Returns: integer value of this measurement, or None if it is
		#	not an integer-valued measurement
		# Assumes: nothing
		# Effects: nothing
		# Throws: nothing

		return self.intValue

	#---------------------------------------------------------------------

	def getFloatValue (self):
		# Purpose: accessor method
		# Returns: float value of this measurement, or None if it is
		#	not a float-valued measurement
		# Assumes: nothing
		# Effects: nothing
		# Throws: nothing

		return self.floatValue

#-----------------------------------------------------------------------------

class Statistic:
	# Is: a measurable quantity that we want to monitor by taking repeated
	#	measurements over time
	# Has: name, abbreviation, list of measurements, list of groups it
	#	belongs to, SQL for computing a measurement, definition, bits
	#	to indicate whether its measurements are integers or floats
	#	and whether it is a statistic for public display or private
	# Does: provides accessor methods to retrieve attributes, and methods
	#	to set most of those attributes (and automatically save the
	#	changes to the database)

	#---------------------------------------------------------------------

	def __init__ (self,
		abbrev		# string; abbreviation of the desired stat
		):
		# Purpose: constructor
		# Returns: nothing
		# Assumes: nothing
		# Effects: queries the database to populate the object
		# Throws: global 'ERROR' if we have problems interacting with
		#	the database or if given 'abbrev' is not recognized

		# This query may retrieve multiple rows, where the rows are
		# identical except that there is one for each statistic group
		# containing this statistic.

		results = sql('''SELECT *
			FROM MGI_Statistic
			WHERE abbreviation = "%s"''' % abbrev)

		if len(results) < 1:
			raise ERROR, 'Unknown statistic: %s' % abbrev

		self.abbrev = abbrev
		self.name = results[0]['name']
		self.definition = results[0]['definition']
		self.private = results[0]['isPrivate']
		self.hasInt = results[0]['hasIntValue']
		self.statisticKey = results[0]['_Statistic_key']

		return

	#---------------------------------------------------------------------

	###------------------------###
	###--- accessor methods ---###
	###------------------------###

	def getKey (self):
		# Purpose: accessor
		# Returns: string -- unique database key of the statistic
		# Assumes: nothing
		# Effects: nothing
		# Throws: nothing

		return self.statisticKey

	#---------------------------------------------------------------------

	def getAbbrev (self):
		return self.abbrev

	#---------------------------------------------------------------------

	def getName (self):
		# Purpose: accessor
		# Returns: string -- name of the statistic
		# Assumes: nothing
		# Effects: nothing
		# Throws: nothing

		return processMeasurementMarkup(self.name)

	#---------------------------------------------------------------------

	def getDefinition (self):
		# Purpose: accessor
		# Returns: string -- definition of the statistic
		# Assumes: nothing
		# Effects: nothing
		# Throws: nothing

		return self.definition

	#---------------------------------------------------------------------

	def isPrivate (self):
		# Purpose: accessor
		# Returns: integer -- 0 if not private, 1 if private
		# Assumes: nothing
		# Effects: nothing
		# Throws: nothing

		return self.private

	#---------------------------------------------------------------------

	def getGroups (self):
		# Purpose: accessor
		# Returns: list of strings, each of which is the name of one
		#	group which contains this Statistic
		# Assumes: nothing
		# Effects: nothing
		# Throws: nothing

		results = sql ('''SELECT ms.name
			FROM MGI_Set ms,
				MGI_SetMember msm
			WHERE ms._Set_key = msm._Set_key
				AND ms._MGIType_key = (SELECT _MGIType_key
						FROM ACC_MGIType
						WHERE name = "Statistic")
				AND msm._Object_key = %d''' % \
					self.statisticKey)

		# collect all the various groups containing this statistic

		groups = []
		for row in results:
			groups.append (row['name'])
		return groups

	#---------------------------------------------------------------------

	def hasIntValue (self):
		# Purpose: accessor
		# Returns: integer -- 1 if has an int value, 0 if has a float
		# Assumes: nothing
		# Effects: nothing
		# Throws: nothing

		return self.hasInt

	#---------------------------------------------------------------------

	def getLatestMeasurement (self):
		# Purpose: accessor
		# Returns: Measurement object -- the most current one for this
		#	Statistic
		# Assumes: nothing
		# Effects: nothing
		# Throws: nothing

		results = sql ('''SELECT isLatest,
				intValue,
				floatValue,
				to_char(timeRecorded, 'YYYY-MM-DD')
					as timeRecorded
			FROM MGI_Measurement
			WHERE _Statistic_key = %d
				AND isLatest = 1''' % self.statisticKey)

		if not results:
			return None

		latestMeasurement = Measurement (
			results[0]['timeRecorded'],
			self.hasInt,
			results[0]['intValue'],
			results[0]['floatValue']
			)

		return latestMeasurement

	#---------------------------------------------------------------------

	def getSql (self):
		# Purpose: accessor
		# Returns: string -- the SQL command used to get a new value
		#	for this Statistic (if it is generated by a single
		#	SQL command against the database; if not, return None)
		# Assumes: nothing
		# Effects: queries the database to retrieve the SQL
		# Throws: global 'ERROR' if we have problems interacting with
		#	the database

		results = sql ('''SELECT sqlChunk
			FROM MGI_StatisticSql
			WHERE _Statistic_key = %d
			ORDER BY sequenceNum''' % self.statisticKey)

		if not results:
			cmd = None
		else:
			cmd = ''
			for row in results:
				cmd = cmd + row['sqlChunk']
		return cmd

	#---------------------------------------------------------------------

	def getMeasurements (self):
		# Purpose: accessor
		# Returns: list of Measurement objects -- all Measurements for
		#	this Statistic, ordered with oldest first
		# Assumes: nothing
		# Effects: queries the database to retrieve the Measurements
		# Throws: global 'ERROR' if we have problems interacting with
		#	the database

		results = sql ('''SELECT intValue,
					floatValue,
					to_char(timeRecorded, 'YYYY-MM-DD')
						as timeRecorded
				FROM MGI_Measurement
				WHERE _Statistic_key = %d
				ORDER BY timeRecorded''' % self.statisticKey)

		measurements = []
		for row in results:
			measurements.append (Measurement (
				row['timeRecorded'],
				self.hasInt,
				row['intValue'],
				row['floatValue']
				) )
		return measurements

	#---------------------------------------------------------------------

	###-----------------------###
	###--- mutator methods ---###
	###-----------------------###

	def setName (self,
		name		# string : new name for this Statistic
		):
		# Purpose: change the name for this Statistic
		# Returns: nothing
		# Assumes: nothing
		# Effects: alters the name for this Statistic in the database
		# Throws: global 'ERROR' if we have problems interacting with
		#	the database

		self.name = name
		sql ('''UPDATE MGI_Statistic
			SET name = "%s"
			WHERE _Statistic_key = %d''' % (name,
				self.statisticKey))
		return

	#---------------------------------------------------------------------

	def setDefinition (self,
		definition	# string; new definition for this Statistic
		):
		# Purpose: change the definition for this Statistic
		# Returns: nothing
		# Assumes: nothing
		# Effects: alters the definition for this Statistic in the db
		# Throws: global 'ERROR' if we have problems interacting with
		#	the database

		self.definition = definition
		sql ('''UPDATE MGI_Statistic
			SET definition = "%s"
			WHERE _Statistic_key = %d''' % (definition,
				self.statisticKey))
		return

	#---------------------------------------------------------------------

	def setIsPrivate (self,
		private		# integer (0/1); 1 if private, 0 if not
		):
		# Purpose: change the private/public status for this Statistic
		# Returns: nothing
		# Assumes: 'private' is either 0 or 1
		# Effects: alters the public/private for this Statistic in db
		# Throws: global 'ERROR' if we have problems interacting with
		#	the database

		self.private = private
		sql ('''UPDATE MGI_Statistic
			SET private = %d
			WHERE _Statistic_key = %d''' % (private,
				self.statisticKey))
		return

	#---------------------------------------------------------------------

	def setHasIntValue (self,
		hasInt		# integer (0/1); 1 if int, 0 if float
		):
		# Purpose: change whether this Statistic expects integer or
		#	float values in the database
		# Returns: nothing
		# Assumes: 'hasInt' is either 0 or 1
		# Effects: alters the float/int expectation for this Statistic
		#	in the database
		# Throws: global 'ERROR' if we have problems interacting with
		#	the database

		self.hasInt = hasInt
		sql ('''UPDATE MGI_Statistic
			SET hasIntValue = %d
			WHERE _Statistic_key = %d''' % (hasInt,
				self.statisticKey))
		return

	#---------------------------------------------------------------------

	def setSql (self,
		statSql		# string; new SQL command for generating each
				# new value for this Statistic
		):
		# Purpose: change the SQL stored for this Statistic in the
		#	database.  (We store the SQL command that is used to
		#	generate a measurement for the Statistic.)
		# Returns: nothing
		# Assumes: nothing
		# Effects: alters the SQL command associated with this
		#	Statistic in the database
		# Throws: global 'ERROR' if we have problems interacting with
		#	the database

		sql ('''DELETE FROM MGI_StatisticSql 
			WHERE _Statistic_key = %d''' % self.statisticKey)

		# We need to store the SQL in chunks of 255 characters.

		i = 0
		while statSql:
			part = statSql[:255]
			statSql = statSql[255:]
			i = i + 1
			sql ('''INSERT MGI_StatisticSql
				VALUES (%d, %d, "%s")''' % (self.statisticKey,
					i,
					part))
		return

#-----------------------------------------------------------------------------

class StatisticGroup:
	# Is: a grouping of Statistic objects, where we collectively refer to
	#	a group of them by a single name
	# Has: a set of Statistic objects
	# Does: allows one to get a list of Statistics or to set the list of
	#	Statistics that should be part of the group

	#---------------------------------------------------------------------

	def __init__ (self,
		name		# string; name of the statistic group
		):
		# Purpose: constructor
		# Returns: nothing
		# Assumes: nothing
		# Effects: queries the database to populate the object
		# Throws: global 'ERROR' if we have problems interacting with
		#	the database or if given 'name' is not recognized

		results = sql ('''SELECT _Set_key, name
				FROM MGI_Set
				WHERE _MGIType_key = (SELECT _MGIType_key
						FROM ACC_MGIType
						WHERE name = "Statistic")
					AND name = "%s"''' % name)
		if not results:
			raise ERROR, "No statistic group named '%s'" % name
		elif len(results) > 1:
			raise ERROR, "Multiple statistic groups named '%s'" \
				% name

		self.setKey = results[0]['_Set_key']
		self.name = results[0]['name']
		return

	#---------------------------------------------------------------------

	def getKey (self):
		return self.setKey

	#---------------------------------------------------------------------

	def getName (self):
		return self.name

	#---------------------------------------------------------------------

	def getStatistics (self):
		# Purpose: retrieve the Statistics that are part of this group
		# Returns: list of Statistic objects, in proper order for this
		#	group
		# Assumes: nothing
		# Effects: queries the database to find and construct the
		#	Statistic objects
		# Throws: global 'ERROR' if we have problems interacting with
		#	the database

#		cmd = '''SELECT abbreviation
#			FROM MGI_Statistic_View
#			WHERE groupName = "%s"
#			ORDER BY sequenceNum''' % self.name

		cmd = '''select distinct ms.abbreviation, msm.sequenceNum
			from MGI_Set mset,
				MGI_SetMember msm,
				MGI_Statistic ms
			where mset.name = '%s'
				and mset._MGIType_key = (select _MGIType_key
					from ACC_MGIType
					where name = 'Statistic')
				and msm._Set_key = mset._Set_key
				and ms._Statistic_key = msm._Object_key
			order by msm.sequenceNum''' % self.name

		results = sql(cmd)

		statistics = []
		for row in results:
			statistics.append (Statistic(row['abbreviation']))
		return statistics

	#---------------------------------------------------------------------

	def setStatistics (self,
		stats		# list of strings or Statistics; if strings,
				# each is an abbreviation for a Statistic
		):
		# Purpose: reset the list of Statistics for this grouping to
		#	be the given 'stats' in their given order
		# Returns: nothing
		# Assumes: we have write permission for the MGI_SetMember
		#	table in the database
		# Effects: updates data in MGI_SetMember in the database
		# Throws: global 'ERROR' if we have problems interacting with
		#	the database

		# produce a dictionary of the desired statistics and their
		# given ordering

		desired = {}	# maps statistic key to sequence number
		i = 0		# increments to track ordering of 'stats'
		for s in stats:
			i = i + 1
			
			# if given a string, we need to retrieve the statistic
			# to find its key

			if type(s) == types.StringType:
				stat = Statistic(s)
				desired[stat.getKey()] = i

			# if given an instance of the Statistic class, we can
			# find its key directly

			elif (type(s) == types.InstanceType) and \
				(s.__class__.__name__ == 'Statistic'):
					desired[s.getKey()] = i
			else:
				raise ERROR, 'Unexpected item type: %s' % \
					str(type(s))
		
		# get a dictionary of the existing statistics for this group
		# and their ordering

		results = sql ('''SELECT _SetMember_key,
					_Object_key, 
					sequenceNum
				FROM MGI_SetMember
				WHERE _Set_key = %s''' % self.setKey)

		existing = {}	# maps statistic key to (sequence num,
				# set member key)

		for row in results:
			existing[row['_Object_key']] = (row['sequenceNum'],
				row['_SetMember_key'])

		# process deletions first, in batches of 100 (if we ever have
		# more than 100 deletions to do...)

		toDelete = []
		for (statKey, (seqNum, smKey)) in existing.items():
			if not desired.has_key(statKey):
				toDelete.append (smKey)
		while toDelete:
			subset = toDelete[:100]
			toDelete = toDelete[100:]

			sql ('''DELETE FROM MGI_SetMember
				WHERE _SetMember_key IN (%s)''' % 
					','.join (map (str, subset)))

		# find highest _SetMember_key stored so far (insertions will
		# need to go up from this)

		results = sql('SELECT MAX(_SetMember_key) FROM MGI_SetMember')
		if not results:
			smKey = 0
		else:
			smKey = results[0]['']

		# process updates and additions next

		sqlUpdate = '''UPDATE MGI_SetMember
			SET sequenceNum = %d
			WHERE _SetMember_key = %d'''

		sqlInsert = '''INSERT MGI_SetMember (_SetMember_key, _Set_key,
					_Object_key, sequenceNum)
				VALUES (%d, %d, %d, %d)'''
			
		for (statKey, seqNum) in desired.items():

			# if we already have this statistic in the group, and
			# if its ordering is different, then update the order

			if existing.has_key(statKey):
				if seqNum != existing[statKey][0]:
					sql(sqlUpdate % (seqNum, 
						existing[statKey][1]))
			else:
				# this statistic is not currently associated
				# with the group, so add it

				smKey = smKey + 1
				sql(sqlInsert % (smKey, self.setKey, statKey,
					seqNum))
		return

#-----------------------------------------------------------------------------

###--------------------------###
###--- Functions (public) ---###
###--------------------------###

def setSqlFunction (
	sqlFn		# function; properly configured function like db.sql
	):
	# Purpose: initialize this module by passing along its db access fn
	# Returns: nothing
	# Assumes: nothing
	# Effects: updates global 'SQL'
	# Throws: nothing

	global SQL
	SQL = sqlFn
	return

#-----------------------------------------------------------------------------

def getStatistic (
	abbrev		# string; the abbreviation for a Statistic
	):
	# Purpose: retrieve the Statistic for this abbreviation
	# Returns: Statistic object
	# Assumes: nothing
	# Effects: queries the database
	# Throws: global 'ERROR' if there is no statistic for 'abbrev', or if
	#	we have problems querying the database

	return Statistic(abbrev)

#-----------------------------------------------------------------------------

def getStatisticByKey (
	statKey		# integer; the _Statistic_key for a Statistic
	):
	# Purpose: retrieve the Statistic with this 'statKey'
	# Returns: Statistic object
	# Assumes: nothing
	# Effects: queries the database
	# Throws: global 'ERROR' if there is no statistic for 'statKey', or if
	#	we have problems querying the database

	results = sql('''SELECT abbreviation 
		FROM MGI_Statistic 
		WHERE _Statistic_key = %d''' % statKey)
	if not results:
		raise ERROR, 'Unknown statistic key: %d' % statKey
	return Statistic(results[0]['abbreviation'])

#-----------------------------------------------------------------------------

def getStatisticByName (
	statName	# string; name of a statistic
	):
	# Purpose: retrieve the Statistic with this 'statName'
	# Returns: Statistic object
	# Assumes: nothing
	# Effects: queries the database
	# Throws: global 'ERROR' if there is not exactly one statistic for
	#	'statKey', or if we have problems querying the database

	results = sql('''SELECT abbreviation 
		FROM MGI_Statistic 
		WHERE name = "%s"''' % statName)

	if not results:
		raise ERROR, 'Unknown statistic name: %s' % statName
	elif len(results) > 1:
		raise ERROR, 'Non-unique statistic name: %s' % statName

	return Statistic(results[0]['abbreviation'])

#-----------------------------------------------------------------------------

def getStatistics (
	groupName = None	# string; name of a statistic group
	):
	# Purpose: retrieve a list of Statistics
	# Returns: list of Statistic objects, either all Statistic objects (if
	#	'groupName' is None) or all the Statistics for the specified
	#	'groupName' (if not None).  Returns an empty list if there are
	#	no statistics associated with the given 'groupName'.
	# Assumes: nothing
	# Effects: queries the database
	# Throws: global 'ERROR' if we have problems querying the database
	# Notes: If we return Statistics for a particular group, they will be
	#	ordered by their sequenceNum for that group.  If we return all
	#	Statistics, they will be sorted by their abbreviations.

	if groupName:
		group = StatisticGroup(groupName)
		return group.getStatistics()

	cmd = '''SELECT abbreviation
			FROM MGI_Statistic
			ORDER BY abbreviation'''
	results = sql(cmd)

	statistics = []
	for row in results:
		statistics.append (Statistic(row['abbreviation']))
	return statistics

#-----------------------------------------------------------------------------

def getAllGroups ():
	# Purpose: retrieve all valid Statistic group names
	# Returns: list of strings, each of which is the name of a stat group
	# Assumes: nothing
	# Effects: queries the database
	# Throws: global 'ERROR' if we have problems querying the database

	results = sql('''SELECT name
		FROM MGI_Set
		WHERE _MGIType_key = (SELECT _MGIType_key
				FROM ACC_MGIType
				WHERE name = "Statistic")
		ORDER BY sequenceNum''')
	groups = []
	for row in results:
		groups.append (row['name'])
	return groups

#-----------------------------------------------------------------------------

def getAllAbbrev ():
	# Purpose: retrieve all valid Statistic abbreviations
	# Returns: list of strings, each of which is the abbreviation for a
	#	Statistic (these are unique; each Statistic has its own
	#	distinct abbreviation)
	# Assumes: nothing
	# Effects: queries the database
	# Throws: global 'ERROR' if we have problems querying the database

	results = sql('''SELECT abbreviation
		FROM MGI_Statistic
		ORDER BY abbreviation''')
	abbreviations = []
	for row in results:
		abbreviations.append (row['abbreviation'])
	return abbreviations

#-----------------------------------------------------------------------------

def createStatisticGroup (
	groupName		# string; name of the statistic group
	):
	# Purpose: create a new StatisticGroup with the given name
	# Returns: the new StatisticGroup object
	# Assumes: nothing
	# Effects: updates the database
	# Throws: global 'ERROR' if there are problems creating the new
	#	StatisticGroup in the database (because of permissions, or
	#	because	there already is a StatisticGroup with that name)

	results = sql('''SELECT _MGIType_key 
			FROM ACC_MGIType 
			WHERE name = "Statistic"''')
	if (not results):
		raise ERROR, 'Unknown MGI Type: Statistic'

	statisticType = results[0]['_MGIType_key']

	results = sql('''SELECT _Set_key
		FROM MGI_Set
		WHERE _MGIType_key = %d
			AND name = "%s"''' % (statisticType, groupName))
	if results:
		raise ERROR, 'Group name "%s" already exists' % groupName

	results = sql('SELECT MAX(_Set_key) FROM MGI_Set')
	if (not results) or (not results[0]['']):
		setKey = 1
	else:
		setKey = results[0][''] + 1

	results = sql('SELECT MAX(sequenceNum) FROM MGI_Set')
	if (not results) or (not results[0]['']):
		seqNum = 1
	else:
		seqNum = results[0][''] + 1

	sql('''INSERT MGI_Set (_Set_key, _MGIType_key, name, sequenceNum)
		VALUES (%d, %d, "%s", %d)''' % \
			(setKey, statisticType, groupName, seqNum))
	return StatisticGroup(groupName)

#-----------------------------------------------------------------------------

def createStatistic (
	abbrev, 	# string; abbreviation for new statistic
	statName, 	# string; name for new statistic
	statDef, 	# string; definition for new statistic
	isPrivate, 	# integer (0/1); is new statistic private?
	hasIntValue	# integer (0/1); expect int values (1) or float (0)?
	):
	# Purpose: create a new Statistic with the given attributes
	# Returns: the new Statistic object
	# Assumes: nothing
	# Effects: updates the database
	# Throws: global 'ERROR' if there are problems creating the new
	#	Statistic in the database (because of permissions, or because
	#	there already is a Statistic with that abbreviation)

	# check to make sure we don't already have one with that abbreviation

	results = sql('''SELECT _Statistic_key
		FROM MGI_Statistic
		WHERE abbreviation = "%s"''' % abbrev)
	if results:
		raise ERROR, "Attempted to add an existing abbreviation: %s" \
				% abbrev

	# find what the next available statistic key would be

	results = sql('SELECT MAX(_Statistic_key) FROM MGI_Statistic')
	if (not results) or (not results[0]['']):
		statKey = 1
	else:
		statKey = results[0][''] + 1

	# update the database, then build and return an object

	sql('''INSERT MGI_Statistic (_Statistic_key, name, abbreviation,
			definition, isPrivate, hasIntValue)
		VALUES (%d, "%s", "%s", "%s", %d, %d)''' % (statKey,
			statName, abbrev, statDef, isPrivate, hasIntValue))
	return Statistic(abbrev)

#-----------------------------------------------------------------------------

def buildTableRows (
	statList	# list of Statistic objects
	):
	# Purpose: build rows from 'statList' that are suitable for the body
	#	of a table object (from the table.py module).  By simply
	#	building the body rows, you can apply whatever formatting you
	#	would like.
	# Returns: list of lists, each sublist is a single row for the table
	# Assumes: nothing
	# Effects: nothing
	# Throws: nothing
	# Notes: float values are displayed out to three decimal places.

	rows = []
	for stat in statList:
		row = [ stat.getName() ]
		latest = stat.getLatestMeasurement()
		if latest:
			if stat.hasIntValue():
				row.append (str(latest.getIntValue()))
			elif latest.getFloatValue() != None:
				row.append ('%4.3f' % latest.getFloatValue())
			else:
				row.append ('None')
			rows.append (row)
	return rows

#-----------------------------------------------------------------------------

def recordMeasurement (
	abbrev, 	# string; abbreviation for the desired Statistic
	intValue, 	# integer; value measured for int-valued Statistic
	floatValue	# float; value measured for float-valued Statistic
	):
	# Purpose: add a new measurement for the Statistic identified by the
	#	given 'abbrev'
	# Returns: nothing
	# Assumes: nothing
	# Effects: updates the database
	# Throws: global 'ERROR' if there are problems adding the new
	#	measurement to the database
	# Notes: Either 'intValue' or 'floatValue' should be non-null, but not
	#	both.

	if intValue != None:
		sql ('exec MGI_recordMeasurement "%s", %d' % (
			abbrev, intValue))
	else:
		sql ('exec MGI_recordMeasurement "%s", null, %1.5f' % (
			abbrev, floatValue))
	return

#-----------------------------------------------------------------------------

def measureAllHavingSql ():
	# Purpose: to record a new Measurement for each Statistic which has
	#	SQL stored in the database for its computation
	# Returns: nothing
	# Assumes: integer valued field will be named 'intValue', or float
	#	valued field will be named 'floatValue'
	# Effects: adds records to MGI_Measurement table in the database
	# Throws: global 'ERROR' if there are problems adding the new
	#	measurements to the database

	failedAbbrev = []	# list of abbreviations with failures

	for abbrev in getAllAbbrev():

		# for each abbreviation, get the Statistic, then get its SQL.
		# Execute the SQL and extract the value measured, then record
		# it in the MGI_Measurement table.

		try:
		    stat = Statistic(abbrev)
		    cmd = stat.getSql()
		    if cmd:
			results = sql(cmd)
			if results:
				# assume that the first fieldname is desired,
				# and get the value measured

				fieldname = results[0].keys()[0]
				value = results[0][fieldname]

				if stat.hasIntValue():
					recordMeasurement(abbrev, value, None)
				else:
					recordMeasurement(abbrev, None, value)
		except:
			failedAbbrev.append (abbrev)
	if failedAbbrev:
		raise ERROR, 'Failed to add measurements for statistics: %s' \
			% ', '.join (failedAbbrev)
	return

#-----------------------------------------------------------------------------

def commaDelimit (
	s	# string; contains an integer or float, represented as string
	):
	# Purpose: take the integer or float value represented in string 's',
	#	and add commas to separate every 3 place values of the whole
	#	number portion
	# Returns: string; see Purpose
	# Assumes: nothing
	# Effects: nothing
	# Throws: global 'ERROR' if there are problems identifying the number
	#	represented by 's'

	# check that we have a valid input

	try:
		checkFloat = float(s)
	except ValueError:
		raise ERROR, 'Cannot add commas to non-numeric: %s' % s

	# break the input into the fractional piece and the whole number piece

	decimalPos = s.find ('.')
	if decimalPos != -1:
		fraction = s[decimalPos:]
		whole = s[:decimalPos]
	else:
		fraction = ''
		whole = s

	# handle the case where the number begins with the decimal point

	if not whole:
		whole = '0'

	# break the whole number portion into 3-digit chunks

	parts = []
	while whole:
		parts.insert (0, whole[-3:])
		whole = whole[:-3]

	# re-assemble and return

	return ','.join(parts) + fraction

#-----------------------------------------------------------------------------

def processMeasurementMarkup (
	s,		    # string which may contain \Measurement() markup
	format = '%0.3f'    # format specifier for float-valued measurement
	):
	# Purpose: to convert any \Measurement() tags in 's' to the most
	#	recent value for their specified statistic abbreviations.
	# Returns: string, like 's' but with substitutions as needed
	# Assumes: tag format will be \Measurement(abbrev)
	# Effects: queries the database to get the latest measurement
	# Throws: global 'ERROR' if there are problems finding a measurement
	#	for an abbreviation
	# Notes: The 'format' parameter is used to convert any float-valued
	#	measurements to strings; then appropriate commas are added
	#	for integer place value separators.
	
	startTag = '\\Measurement('	# how does the markup start?
	tagLen = len(startTag)		# length of string we'll search for

	pos = s.find(startTag)

	if pos == -1:			# no markup -- return as-is
		return s

	t = ''		# string we're building; new version of 's'
	lastPos = 0	# last position in 's' that we processed
	lens = len(s)	# length of input string

	# continue for all markups in 's'

	while pos != -1:
		# in order to handle parentheses in the statistic abbreviation
		# we need to start counting parentheses after the start tag we
		# found.  Once we hit the end of the string or a matching
		# close parenthesis, then we're done.

		y = pos + tagLen
		pCount = 1
		while (pCount > 0) and (y < lens):
			ch = s[y]
			if ch == ')':
				pCount = pCount - 1
			elif ch == '(':
				pCount = pCount + 1
			y = y + 1

		if pCount == 0:
			end = y - 1		# end of this markup
		else:
			# mismatched parentheses; return as-is
			return s

		abbrev = s[pos + tagLen:end]	# stat abbreviation
		stat = Statistic(abbrev)	# the Statistic itself

		measurement = stat.getLatestMeasurement()
		if measurement.hasIntValue():
			mValue = str(measurement.getIntValue())
		else:
			mValue = format % measurement.getFloatValue()

		# add any unprocessed characters before the markup, then the
		# value of the statistic to replace the markup

		t = t + s[lastPos:pos]
		t = t + commaDelimit (mValue)

		# and, go back to look for more markups

		lastPos = end + 1
		pos = s.find (startTag, end + 1)

	# add any unprocessed characters after the last markup
	t = t + s[lastPos:]

	return t
