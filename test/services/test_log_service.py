#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import unittest
import json

from postgresql_log_parser.services import LogParserService
from postgresql_log_parser.parsers import PyParsingParser, RegexpParser
from postgresql_log_parser.repositories import InMemoryRepository

from ..test_helper import fixture_path

class LogParserServiceTestCase(unittest.TestCase):

    def setUp(self):
        self.parser_pyparsing = PyParsingParser()
        self.parser_regexp = RegexpParser()
        self.repository = InMemoryRepository()
        self.service_pyparsing = LogParserService(self.repository, parser=self.parser_pyparsing)
        self.service_regexp = LogParserService(self.repository, parser=self.parser_regexp)

    def test_postgresql_log_file_pyparsing(self):
        self.service_pyparsing.parse_file(fixture_path('query.txt'))
        parsed_data = self.repository.get_all()
        self.assertEqual(len(parsed_data), 3)
        data_0 = json.loads(parsed_data[0])
        self.assertEqual(data_0['timestamp'], '2017-02-09 08:17:40 GMT')
        self.assertEqual(data_0['pid'], '9796')
        self.assertEqual(data_0['pid_part'], '8097')
        self.assertEqual(data_0['duration'], '0.187')
        self.assertEqual(data_0['query'], "SELECT * FROM (SELECT the_geom_webmercator, area FROM green_areas_zoomed('3.40282e+38',ST_MakeEnvelope(-3.402823466385289e+38,-3.402823466385289e+38,3.402823466385289e+38,3.402823466385289e+38,3857)) AS cdbq LIMIT 0")
        data_1 = json.loads(parsed_data[2])
        self.assertEqual(data_1['timestamp'], '2017-02-09 08:17:07 GMT')
        self.assertEqual(data_1['pid'], '9795')
        self.assertEqual(data_1['pid_part'], '12741')
        self.assertEqual(data_1['duration'], '0.096')
        self.assertEqual(data_1['query'], "SELECT usename, passwd FROM pg_shadow WHERE usename=cartodb_user_01df4999-81aa-4135-b460-1e5b8a7f7f79 AND wadus=1000")

    def test_postgresql_log_file_regexp(self):
        self.service_regexp.parse_file(fixture_path('query.txt'))
        parsed_data = self.repository.get_all()
        self.assertEqual(len(parsed_data), 3)
        data_0 = json.loads(parsed_data[0])
        self.assertEqual(data_0['timestamp'], '2017-02-09 08:17:40')
        self.assertEqual(data_0['pid'], '9796')
        self.assertEqual(data_0['pid_part'], '8097')
        self.assertEqual(data_0['duration'], '0.187')
        self.assertEqual(data_0['query'], "SELECT * FROM (SELECT the_geom_webmercator, area FROM green_areas_zoomed('3.40282e+38',ST_MakeEnvelope(-3.402823466385289e+38,-3.402823466385289e+38,3.402823466385289e+38,3.402823466385289e+38,3857)) AS cdbq LIMIT 0")
        data_1 = json.loads(parsed_data[2])
        self.assertEqual(data_1['timestamp'], '2017-02-09 08:17:07')
        self.assertEqual(data_1['pid'], '9795')
        self.assertEqual(data_1['pid_part'], '12741')
        self.assertEqual(data_1['duration'], '0.096')
        self.assertEqual(data_1['query'], "SELECT usename, passwd FROM pg_shadow WHERE usename='cartodb_user_01df4999-81aa-4135-b460-1e5b8a7f7f79' AND wadus='1000'")

    def test_postgresql_log_file_with_mixed_parts_merged_correctly(self):
        self.service_regexp.parse_file(fixture_path('mixed_parts_log.txt'))
        parsed_data = self.repository.get_all()
        self.assertEqual(len(parsed_data), 3)
        data = json.loads(parsed_data[2])
        self.assertEqual(data['timestamp'], '2017-02-09 08:17:40')
        self.assertEqual(data['pid'], '9796')
        self.assertEqual(data['pid_part'], '8093')
        self.assertEqual(data['duration'], '0.168')
        self.assertEqual(data['query'], "SELECT * FROM (SELECT cartodb_id, scalerank, the_geom_webmercator FROM urban_areas_zoomed('3.40282e+38',ST_MakeEnvelope(-3.402823466385289e+38,-3.402823466385289e+38,3.402823466385289e+38,3.402823466385289e+38,3857)) AS _ ) as cdbq LIMIT 65535")

    def test_user_queries_log_are_processed_correctly_with_regexp(self):
        self.service_regexp.parse_file(fixture_path('user_queries.txt'))
        parsed_data = self.repository.get_all()
        self.assertEqual(len(parsed_data), 2)
        data = json.loads(parsed_data[1])
        self.assertEqual(data['timestamp'], '2017-03-27 11:44:44')
        self.assertEqual(data['pid'], '13993')
        self.assertEqual(data['pid_part'], '2254')
        self.assertEqual(data['duration'], '2465.723')
        self.assertEqual(data['query'], "SELECT ST_AsTWKB(ST_Simplify(ST_RemoveRepeatedPoints(\"the_geom_webmercator\",10000),10000,true),-4) AS geom FROM (select c.the_geom, st_transform(st_makevalid(c.the_geom_webmercator), 3786) as the_geom_webmercator, s.iso, s.score FROM scores as s JOIN countries as c on c.iso = s.iso WHERE indicator_slug='health_equality' ) as cdbq WHERE \"the_geom_webmercator\" && ST_SetSRID('BOX3D(-2504688.542848656 -20037508.3,20037508.3 2504688.542848656)'::box3d, 3857)")

    def test_user_insert_queries_log_are_processed_correctly_with_regexp(self):
        self.service_regexp.parse_file(fixture_path('insert_log.txt'))
        parsed_data = self.repository.get_all()
        self.assertEqual(len(parsed_data), 3)
        data = json.loads(parsed_data[0])
        self.assertEqual(data['timestamp'], '2017-04-04 04:55:07')
        self.assertEqual(data['pid'], None)
        self.assertEqual(data['pid_part'], '100')
        self.assertEqual(data['duration'], '100.247')
        self.assertEqual(data['query'], "INSERT INTO ldata (the_geom, name, serial, time) VALUES (ST_GeomFromGeoJSON('{\"type\":\"Point\",\"coordinates\":[-8.706771,42.672206],\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:4326\"}}}'), 'CM011', '0100320141200101', 'Tue, 04 Apr 2017 04:55:07 GMT'::timestamp) ON CONFLICT ON CONSTRAINT serial_unique DO NOTHING")
        data = json.loads(parsed_data[1])
        self.assertEqual(data['timestamp'], '2017-04-04 04:55:07')
        self.assertEqual(data['pid'], None)
        self.assertEqual(data['pid_part'], '43')
        self.assertEqual(data['duration'], '142.026')
        self.assertEqual(data['query'], "INSERT INTO ldata (the_geom, name, serial, time) VALUES (ST_GeomFromGeoJSON('{\"type\":\"Point\",\"coordinates\":[-6.369976,39.460876],\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:4326\"}}}'), 'CM066', '0100320151200181', 'Tue, 04 Apr 2017 04:55:07 GMT'::timestamp) ON CONFLICT ON CONSTRAINT serial_unique DO NOTHING")

    def test_user_parse_resque_log_correctly_with_regexp(self):
        self.service_regexp.parse_file(fixture_path('resque_log.txt'))
        parsed_data = self.repository.get_all()
        self.assertEqual(len(parsed_data), 1)
        data = json.loads(parsed_data[0])
        self.assertEqual(data['timestamp'], '2017-04-04 04:17:28')
        self.assertEqual(data['pid'], '28752')
        self.assertEqual(data['pid_part'], '1456')
        self.assertEqual(data['duration'], '5.163')
        self.assertEqual(data['query'], "SELECT public.AddGeometryColumn( 'cdb_importer','importer_9ce2d88618ed11e7ada70ef7f98ade21','the_geom',4326,'geometry',2 );")

    def test_user_parse_multipart_without_011_string_with_regexp(self):
        self.service_regexp.parse_file(fixture_path('multipart_without_011.txt'))
        parsed_data = self.repository.get_all()
        self.assertEqual(len(parsed_data), 1)
        data = json.loads(parsed_data[0])
        self.assertEqual(data['timestamp'], '2017-04-04 04:54:50')
        self.assertEqual(data['pid'], '20373')
        self.assertEqual(data['pid_part'], '49')
        self.assertEqual(data['duration'], '21.588')
        self.assertEqual(data['query'], 'SELECT "pg_attribute"."attname" AS "name", CAST("pg_attribute"."atttypid" AS integer) AS "oid", format_type("pg_type"."oid", "pg_attribute"."atttypmod") AS "db_type", pg_get_expr("pg_attrdef"."adbin", "pg_class"."oid") AS "default", NOT "pg_attribute"."attnotnull" AS "allow_null", COALESCE(("pg_attribute"."attnum" = ANY("pg_index"."indkey")), false) AS "primary_key", "pg_namespace"."nspname" FROM "pg_class" INNER JOIN "pg_attribute" ON ("pg_attribute"."attrelid" = "pg_class"."oid") INNER JOIN "pg_type" ON ("pg_type"."oid" = "pg_attribute"."atttypid") INNER JOIN "pg_namespace" ON ("pg_namespace"."oid" = "pg_class"."relnamespace") LEFT OUTER JOIN "pg_attrdef" ON (("pg_attrdef"."adrelid" = "pg_class"."oid") AND ("pg_attrdef"."adnum" = "pg_attribute"."attnum")) LEFT OUTER JOIN "pg_index" ON (("pg_index"."indrelid" = "pg_class"."oid") AND ("pg_index"."indisprimary" IS TRUE)) WHERE (("pg_attribute"."attisdropped" IS FALSE) AND ("pg_attribute"."attnum" > 0) AND ("pg_class"."relname" = \'guns_n_roses_in_chicago\') AND ("pg_namespace"."nspname" = \'public\')) ORDER BY "pg_attribute"."attnum"')