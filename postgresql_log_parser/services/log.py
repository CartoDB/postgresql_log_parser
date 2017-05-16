import json
import re
import hashlib
import os
import gzip

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
        open_fn = open
        if file_name[-2:] == 'gz':
            open_fn = gzip.open
        print 'processing: {0}'.format(file_name)
        with open_fn(file_name, mode='r+b') as f:
            for line in f:
                parsed_line = self.parser.parse_line(line)
                if 'pid' in parsed_line and 'pid_part' in parsed_line:
                    hashkey = self.__initialize_and_generate_hash(parsed_line)
                    self.process_buffer[hashkey].append(parsed_line)
        self.__process_lines()

    def __initialize_and_generate_hash(self, parsed_line):
        hashkey = self.__generate_hash(parsed_line).hexdigest()[:12]
        if hashkey not in self.process_buffer:
            self.process_buffer[hashkey] = []
            return hashkey
        elif int(parsed_line['part']) == 1 and hashkey in self.process_buffer:
            index_hash = 2
            hashkey = self.__generate_hash(parsed_line, index_hash).hexdigest()[:12]
            # Sometimes we have collision in hashkeys because there isn't
            # a PID, the timestamp and pid_part are the same and we can
            # have an unknown number of cases so we add an index at the end
            # of string we use to generate the string
            while hashkey in self.process_buffer:
                index_hash += 1
                hashkey = self.__generate_hash(parsed_line, index_hash).hexdigest()[:12]
            self.process_buffer[hashkey] = []
            return hashkey
        elif int(parsed_line['part']) > 1 and hashkey in self.process_buffer:
            # We look for the last hashkey we have for an aggregated multipart
            index_hash = 2
            while True:
                hashkey_new = self.__generate_hash(parsed_line, index_hash).hexdigest()[:12]
                if hashkey_new not in self.process_buffer:
                    return hashkey
                hashkey = hashkey_new
                index_hash += 1

    def __generate_hash(self, parsed_line, index=1):
        pid = parsed_line['pid']
        pid_part = parsed_line['pid_part']
        date = parsed_line['date']
        if pid:
            return hashlib.sha256(b"{0}_{1}_{2}_{3}".format(pid, pid_part, date, index))
        else:
            return hashlib.sha256(b"{0}_{1}_{2}".format(date, pid_part, index))

    def __process_lines(self):
        for _, lines in self.process_buffer.iteritems():
            if len(lines) > 1:
                line = self.__process_multipart_line(lines)
            else:
                line = lines[0]
                if 'query' in line:
                    line['query'] = line['query'].strip()
                else:
                    print 'not query: {0}'.format(line)
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
            first_line['query'] = " ".join(query).strip()
        elif len(parameters) > 0:
            query = first_line['query']
            for key, value in parameters.iteritems():
                query = query.replace(key, value)
            first_line['query'] = query.strip()
        return first_line

    def __flush_storage_buffer(self, buffer_limit=0):
        if len(self.storage_buffer) >= buffer_limit:
            self.repository.store(self.storage_buffer)
            self.storage_buffer = []
