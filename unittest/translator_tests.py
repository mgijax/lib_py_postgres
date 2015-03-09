#!/usr/local/bin/python
#
# Unit tests for the Sybase -> Postgres 
#	translators
# 
#

import sys,os.path
# adjust the path for running the tests locally, so that it can find the modules (1 dir up)
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import unittest

from pg_db import translate, translate_be

class SybaseAllTranslatorTest(unittest.TestCase):
	"""
	Test back end translations
	
	Note: Please test any new translations here
		to ensure backward compatibility
	"""

	def translateAll(self, sql):
		"""
		For these tests we want to 
		    run both translator functions
		"""
		sql = translate(sql)
		sql = translate_be(sql)
		return sql

	### Test some sanity clauses ###
	
	def test_simple_select(self):
		sql = '''
			select * 
			from mgi_dbinfo	
		'''
		expected = sql
		self.assertEquals(expected, self.translateAll(sql))

	def test_simple_select2(self):
		sql = '''
			SELECT symbol, name,
				m._marker_key, creation_date,
				mn.note	
			FROM mrk_marker m join
			    marker_notes mn on
				m._marker_key = mn._marker_key
			where m._organism_key = 1
		'''
		expected = sql
		self.assertEquals(expected, self.translateAll(sql))

	### Test name = column syntax in sybase ###

	def test_column_equals_syntax(self):
		sql = '''
			select alleleKey=_allele_key
			from all_allele
		'''
		expected = '''
			select _allele_key as alleleKey
			from all_allele
		'''
		self.assertEquals(expected, self.translateAll(sql))

	def test_column_equals_syntax_many(self):
		sql = '''
			select alleleKey=_allele_key,
				alleleName = name,
				testString = "test"
			from all_allele
		'''
		expected = '''
			select _allele_key as alleleKey,
				name as alleleName,
				'test' as testString
			from all_allele
		'''
		self.assertEquals(expected, self.translateAll(sql))

	def test_column_equals_syntax_with_function(self):
		sql = '''
			select maxKey=max(_allele_key)
			from all_allele
		'''
		expected = '''
			select max(_allele_key) as maxKey
			from all_allele
		'''
		self.assertEquals(expected, self.translateAll(sql))

	### Test exec a stored procedure ###
	def test_stored_procedure_call(self):
		sql = '''exec GXD_doAssayStuff 1001'''
		expected = '''select * from GXD_doAssayStuff (1001);'''
		self.assertEquals(expected, self.translateAll(sql))


	### Test miscellaneous conversions ###
	def test_char_date_conversion(self):
		# From MRK_GOUnknown.sql
		sql = '''
		select distinct substring(m.symbol,1,25) as symbol, m._Marker_key, r._Refs_key,
		    convert(char(10), rr.creation_date, 101) as jnumDate,
		    convert(char(10), a.creation_date, 101) as annotDate
		'''
		expected = '''
		select distinct substr(m.symbol,1,25) as symbol, m._Marker_key, r._Refs_key,
		    to_char( rr.creation_date, 'MM/dd/yyyy') as jnumDate,
		    to_char( a.creation_date, 'MM/dd/yyyy') as annotDate
		'''
		self.assertEquals(expected, self.translateAll(sql))

	
	def test_dateadd(self):
		# From GO_done
		sql = '''
		select b._Marker_key, b.jnumID 
		from BIB_GOXRef_View b, #goref g
		where b._Marker_key = g._Marker_key
		and exists (select 1 from BIB_GOXRef_View b, #godone g
			 where b._Marker_key = g._Marker_key
			 and b.creation_date > dateadd(day, 1, g.cdate))
		'''
		expected = '''
		select b._Marker_key, b.jnumID 
		from BIB_GOXRef_View b, goref g
		where b._Marker_key = g._Marker_key
		and exists (select 1 from BIB_GOXRef_View b, godone g
			 where b._Marker_key = g._Marker_key
			 and b.creation_date > (g.cdate + interval '1 day'))
		'''
		self.assertEquals(expected, self.translateAll(sql))

	def test_offset_column(self):
		sql = '''
			select offset from
			mrk_location
		'''
		expected = '''
			select cmOffset from
			mrk_location
		'''
		self.assertEquals(expected, self.translateAll(sql))

	def test_like_operator(self):
		sql = '''
			select * from
			mrk_marker
			where symbol like 'Pa%'
		'''
		expected = '''
			select * from
			mrk_marker
			where symbol ilike 'Pa%'
		'''
		self.assertEquals(expected, self.translateAll(sql))

	def test_is_null(self):
		sql = '''
			select * from
			gxd_assay
			where _reportergene_key = NULL
		'''
		expected = '''
			select * from
			gxd_assay
			where _reportergene_key is null
		'''
		self.assertEquals(expected, self.translateAll(sql))

	def test_isnot_null(self):
		sql = '''
			select * from
			gxd_assay
			where _reportergene_key != NULL
		'''
		expected = '''
			select * from
			gxd_assay
			where _reportergene_key is not null
		'''
		self.assertEquals(expected, self.translateAll(sql))

	def test_null_update(self):
		sql = '''
			update mrk_marker
			set name = NULL
		'''
		expected = sql
		self.assertEquals(expected, self.translateAll(sql))

	### test string equals in where clause  ###
	def test_string_equals_insensitive(self):
		sql = '''
			select * from
			mrk_marker
			where symbol = 'pax6'
		'''
		expected = '''
			select * from
			mrk_marker
			where lower(symbol) = 'pax6'
		'''
		self.assertEquals(expected, self.translateAll(sql))

	def test_string_in_insensitive(self):
		sql = '''
			select * from
			mrk_marker
			where symbol in ('pax6','kit')
			and name not in ('agouti','hox')
		'''
		expected = '''
			select * from
			mrk_marker
			where lower(symbol) in ('pax6','kit')
			and lower(name) not in ('agouti','hox')
		'''
		self.assertEquals(expected, self.translateAll(sql))


	### Test Temp Tables ###
	def test_select_temp_table(self):
		sql = '''
			select _marker_key
			into #markerKeys
			from mrk_marker
		'''
		expected = '''
			select _marker_key
			INTO TEMPORARY TABLE markerKeys
			from mrk_marker
		'''
		self.assertEquals(expected, self.translateAll(sql))


	### handle table deletes ###
	def test_delete_using(self):
		sql = '''
		delete from mrk_marker
		from prb_probe_marker pm
		where pm._marker_key=mrk_marker._marker_key
		'''
		expected = '''
		delete from mrk_marker
		USING prb_probe_marker pm
		where pm._marker_key=mrk_marker._marker_key
		'''
		self.assertEquals(expected, self.translateAll(sql))
		

if __name__ == '__main__':
        unittest.main()
