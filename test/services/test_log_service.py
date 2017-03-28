#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import unittest

from postgresql_log_parser.services import LogParserService, LogDataExtractionService
from postgresql_log_parser.parsers import PyParsingParser, RegexpParser
from postgresql_log_parser.repositories import InMemoryRepository

from ..test_helper import fixture_path

class LogParserServiceTestCase(unittest.TestCase):

    def setUp(self):
        self.parser_pyparsing = PyParsingParser()
        self.parser_regexp = RegexpParser()
        self.repository = InMemoryRepository()
        self.service_pyparsing = LogParserService(parser=self.parser_pyparsing,
                                                  repository=self.repository)
        self.service_regexp = LogParserService(parser=self.parser_regexp,
                                               repository=self.repository)

    def test_postgresql_log_file_pyparsing(self):
        self.service_pyparsing.parse(fixture_path('query.txt'))
        parsed_data = self.repository.get_all()
        self.assertEqual(len(parsed_data), 2)

    def test_postgresql_log_file_regexp(self):
        self.service_regexp.parse(fixture_path('query.txt'))
        parsed_data = self.repository.get_all()
        self.assertEqual(len(parsed_data), 2)

    def test_postgresql_log_file_with_mixed_parts_merged_correctly(self):
        self.service_regexp.parse(fixture_path('mixed_parts_log.txt'))
        parsed_data = self.repository.get_all()
        self.assertEqual(len(parsed_data), 3)

    def test_user_queries_log_is_processed_correctly_with_regexp(self):
        self.service_regexp.parse(fixture_path('user_queries.txt'))
        parsed_data = self.repository.get_all()
        print(parsed_data)
        self.assertEqual(len(parsed_data), 2)

class LogDataExtractionServiceTestCase(unittest.TestCase):

    def setUp(self):
        self.repository = InMemoryRepository()
        self.service = LogDataExtractionService(repository=self.repository)

    def test_should_extract_data_from_previous_process_correctly(self):
        self.service.process(fixture_path('processed_log.txt'))
        self.assertEqual(len(self.repository.get_all()), 4)

    def test_should_extract_data_from_user_queries_correctly(self):
        self.service.process(fixture_path('user_queries_processed.txt'))
        print(self.repository.get_all())
        self.assertEqual(len(self.repository.get_all()), 2)