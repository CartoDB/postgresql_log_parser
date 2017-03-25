#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import unittest
import json

from ..test_helper import from_fixture
from postgresql_log_parser.parsers import RegexpParser

class RegexpParserTestCase(unittest.TestCase):

    FIXTURE_QUERY = "SELECT * FROM (SELECT the_geom_webmercator, area FROM green_areas_zoomed('3.40282e+38',ST_MakeEnvelope(-3.402823466385289e+38,-3.402823466385289e+38,3.402823466385289e+38,3.402823466385289e+38,3857)) AS cdbq LIMIT 0"

    def setUp(self):
        self.parser = RegexpParser()

    def test_postgresql_log_line_should_be_parsed_correctly(self):
        fixture = from_fixture('query.txt')
        parsed_data = self.parser.parse_line(fixture[0])
        self.assertEqual(parsed_data['part'], '1')
        self.assertEqual(parsed_data['timestamp'], '2017-02-09 08:17:40')
        self.assertEqual(parsed_data['user'], 'cartodb_user_01df4999-81aa-4135-b460-1e5b8a7f7f79')
        self.assertEqual(parsed_data['database'], 'cartodb_user_01df4999-81aa-4135-b460-1e5b8a7f7f79_db')
        self.assertEqual(parsed_data['duration'], '0.187')
        self.assertEqual(parsed_data['command'], 'statement')
        self.assertEqual(parsed_data['query'], self.FIXTURE_QUERY)
        parsed_data = self.parser.parse_line(fixture[1])
        self.assertEqual(parsed_data['part'], '1')
        self.assertEqual(parsed_data['timestamp'], '2017-02-09 08:17:07')
        self.assertEqual(parsed_data['user'], 'postgres')
        self.assertEqual(parsed_data['database'], 'cartodb_user_01df4999-81aa-4135-b460-1e5b8a7f7f79_db')
        self.assertEqual(parsed_data['duration'], '0.096')
        self.assertEqual(parsed_data['command'], 'parse')
        self.assertEqual(parsed_data['query'], 'SELECT usename, passwd FROM pg_shadow WHERE usename=$1 AND wadus=$2')
        parsed_data = self.parser.parse_line(fixture[2])
        self.assertEqual(parsed_data['part'], '2')
        self.assertEqual(parsed_data['timestamp'], '2017-02-09 08:17:07')
        self.assertEqual(parsed_data['user'], 'postgres')
        self.assertEqual(parsed_data['database'], 'cartodb_user_01df4999-81aa-4135-b460-1e5b8a7f7f79_db')
        self.assertEqual(parsed_data['parameters']['$1'], "'cartodb_user_01df4999-81aa-4135-b460-1e5b8a7f7f79'")
        self.assertEqual(parsed_data['parameters']['$2'], "'1000'")

    def test_postgresql_multipart_log_line_should_be_parsed_correctly(self):
        fixture = from_fixture('splitted_query.txt')
        parsed_data = self.parser.parse_line(fixture[0])
        self.assertEqual(parsed_data['part'], '1')
        self.assertEqual(parsed_data['timestamp'], '2017-02-09 08:17:40')
        self.assertEqual(parsed_data['user'], 'cartodb_user_01df4999-81aa-4135-b460-1e5b8a7f7f79')
        self.assertEqual(parsed_data['database'], "cartodb_user_01df4999-81aa-4135-b460-1e5b8a7f7f79_db")
        self.assertEqual(parsed_data['duration'], '0.168')
        self.assertEqual(parsed_data['command'], 'statement')
        self.assertEqual(parsed_data['query'], 'SELECT * FROM (SELECT')
        parsed_data = self.parser.parse_line(fixture[1])
        self.assertEqual(parsed_data['part'], '2')
        self.assertEqual(parsed_data['multipart'], True)
        self.assertEqual(parsed_data['query'], 'cartodb_id,')
        parsed_data = self.parser.parse_line(fixture[2])
        self.assertEqual(parsed_data['part'], '3')
        self.assertEqual(parsed_data['multipart'], True)
        self.assertEqual(parsed_data['query'], 'scalerank,')
        parsed_data = self.parser.parse_line(fixture[3])
        self.assertEqual(parsed_data['part'], '4')
        self.assertEqual(parsed_data['multipart'], True)
        self.assertEqual(parsed_data['query'], 'the_geom_webmercator')
        parsed_data = self.parser.parse_line(fixture[4])
        self.assertEqual(parsed_data['part'], '5')
        self.assertEqual(parsed_data['multipart'], True)
        self.assertEqual(parsed_data['query'], "FROM urban_areas_zoomed('3.40282e+38',ST_MakeEnvelope(-3.402823466385289e+38,-3.402823466385289e+38,3.402823466385289e+38,3.402823466385289e+38,3857)) AS _")
        parsed_data = self.parser.parse_line(fixture[5])
        self.assertEqual(parsed_data['part'], '6')
        self.assertEqual(parsed_data['multipart'], True)
        self.assertEqual(parsed_data['query'], ') as cdbq LIMIT 0')

    def test_postgresl_discard_unuseful_data(self):
        fixture = from_fixture('discarded_queries.txt')
        parsed_data = self.parser.parse_line(fixture[0])
        self.assertEqual(len(parsed_data), 0)
        parsed_data = self.parser.parse_line(fixture[1])
        self.assertEqual(len(parsed_data), 0)
        parsed_data = self.parser.parse_line(fixture[2])
        self.assertEqual(len(parsed_data), 0)
        parsed_data = self.parser.parse_line(fixture[3])
        self.assertEqual(len(parsed_data), 0)
