"""
Log parser service
"""
import mmap
import json

from postgresql_log_parser.parsers import PyParsingParser
from postgresql_log_parser.repositories import FileRepository

class LogService(object):
    """
    Class to parse postgresql logs
    """

    def __init__(self, parser=None, repository=None):
        self.parser = parser or PyParsingParser()
        self.repository = repository or FileRepository()
        self.buffer = []

    def log_parse(self, file_name):
        """
        Method to parse the provided log file
        """
        with open(file_name, mode='r+b') as f:
            m = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
            # We're going to process previous line in order to
            # be able to merge multipart
            first_line = self.parser.parse_line(m.readline())
            current_line_block = [first_line]
            for line in iter(m.readline, ""):
                line = self.parser.parse_line(line)
                if len(line) == 0:
                    continue
                if line['multipart']:
                    current_line_block.append(line)
                else:
                    if len(current_line_block) > 1:
                        multipart_line = self.__process_multipart_line(current_line_block)
                        if self.__valid_line(multipart_line):
                            self.buffer.append(json.dumps(multipart_line))
                    else:
                        current_line = current_line_block[0]
                        if self.__valid_line(current_line):
                            self.buffer.append(json.dumps(current_line))
                            self.__process_lines(1000)
                    current_line_block = [line]
            self.__process_lines(process_all=True)

    def __valid_line(self, line):
        return 'command' in line and line['command'] in ['statement', 'execute']

    def __process_lines(self, process_all=False, buffer_size=1000):
        if process_all or len(self.buffer) % buffer_size == 0:
            self.repository.store(self.buffer)
            self.buffer = []

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
