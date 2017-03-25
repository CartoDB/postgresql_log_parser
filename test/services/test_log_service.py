#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import unittest

from ..test_helper import fixture_path
from postgresql_log_parser.services import LogService
from postgresql_log_parser.parsers import PyParsingParser, RegexpParser

class LogParserTestCase(unittest.TestCase):

    def setUp(self):
        self.parser_pyparsing = PyParsingParser()
        self.parser_regexp = RegexpParser()
        self.service_pyparsing = LogService(parser=self.parser_pyparsing)
        self.service_regexp = LogService(parser=self.parser_regexp)

    def test_postgresql_log_file_pyparsing(self):
        self.service_pyparsing.log_parse(fixture_path('query.txt'))

    def test_postgresql_log_file_regexp(self):
        self.service_regexp.log_parse(fixture_path('query.txt'))