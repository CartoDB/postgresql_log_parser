#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import unittest

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
        self.service_pyparsing.parse(fixture_path('query.txt'))
        parsed_data = self.repository.get_all()
        self.assertEqual(len(parsed_data), 3)

    def test_postgresql_log_file_regexp(self):
        self.service_regexp.parse(fixture_path('query.txt'))
        parsed_data = self.repository.get_all()
        self.assertEqual(len(parsed_data), 3)

    def test_postgresql_log_file_with_mixed_parts_merged_correctly(self):
        self.service_regexp.parse(fixture_path('mixed_parts_log.txt'))
        parsed_data = self.repository.get_all()
        self.assertEqual(len(parsed_data), 3)

    def test_user_queries_log_is_processed_correctly_with_regexp(self):
        self.service_regexp.parse(fixture_path('user_queries.txt'))
        parsed_data = self.repository.get_all()
        self.assertEqual(len(parsed_data), 2)