#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import unittest

from ..test_helper import from_fixture
from postgresql_log_parser.services import LogService
from postgresql_log_parser.parsers import PyParsingParser

class LogParserTestCase(unittest.TestCase):
    
    def setUp(self):
        pattern = '(DISCARD ALL|SET\s|SELECT\s1)'
        self.parser = PyParsingParser(filter_pattern=pattern)
        self.service = LogService(parser=self.parser)

    def test_postgresql_1mb_log_file(self):
        self.service.log_parse('/home/ubuntu/www/misc/basemaps_research/logs/postgresql_1mb.log')
    
    # def test_postgresql_50_log_file(self):
    #     self.service.log_parse('/home/ubuntu/www/misc/basemaps_research/logs/postgresql_50mb.log')
