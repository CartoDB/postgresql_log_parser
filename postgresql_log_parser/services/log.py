import json
import re
import hashlib
import os

from postgresql_log_parser.parsers import RegexpParser

class LogParserService(object):
    """
    Class to parse postgresql logs
    """

    def __init__(self, repository, parser=None):
        self.parser = parser or RegexpParser()
        self.repository = repository
        self.process_buffer = {}
        self.storage_buffer = []

    def parse_files(self, list_files):
        """
        Parse a directory with regexp to select the files to be processed
        """
        for file_name in list_files:
            self.parse_file(file_name)

    def parse_file(self, file_name):
        """
        Parse the log passed as file name argument
        """
        with open(file_name, mode='r+b') as f:
            for line in f:
                parsed_line = self.parser.parse_line(line)
                if 'pid' in parsed_line and 'pid_part' in parsed_line:
                    hashkey = self.__generate_hash(
                        parsed_line['pid'], parsed_line['pid_part']).hexdigest()[:12]
                    if hashkey not in self.process_buffer:
                        self.process_buffer[hashkey] = []
                    self.process_buffer[hashkey].append(parsed_line)
        self.__process_lines()

    def __generate_hash(self, pid, pid_part):
        return hashlib.sha256(b"{0}_{1}".format(pid, pid_part))

    def __process_lines(self):
        for _, lines in self.process_buffer.iteritems():
            if len(lines) > 1:
                line = self.__process_multipart_line(lines)
            else:
                line = lines[0]
            self.storage_buffer.append(json.dumps(line))
            self.__flush_storage_buffer(1000)
        self.process_buffer = {}
        self.__flush_storage_buffer()

    def __process_multipart_line(self, lines):
        first_line = lines[0]
        parameters = {}
        queries = []
        for line in lines[1:]:
            if 'parameters' in line:
                parameters.update(line['parameters'])
            elif 'query' in line:
                queries.append(line['query'])
        if len(queries) > 0:
            query = [first_line['query']]
            query.extend(queries)
            first_line['query'] = " ".join(query)
        elif len(parameters) > 0:
            query = first_line['query']
            for key, value in parameters.iteritems():
                query = query.replace(key, value)
            first_line['query'] = query
        return first_line

    def __flush_storage_buffer(self, buffer_limit=0):
        if len(self.storage_buffer) >= buffer_limit:
            self.repository.store(self.storage_buffer)
            self.storage_buffer = []
