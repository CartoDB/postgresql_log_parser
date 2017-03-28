import json
import re
import hashlib
import psqlparse

from postgresql_log_parser.parsers import RegexpParser
from postgresql_log_parser.repositories import FileRepository
from postgresql_log_parser.utils.geo import GeoUtils

class LogParserService(object):
    """
    Class to parse postgresql logs
    """

    def __init__(self, repository, parser=None):
        self.parser = parser or RegexpParser()
        self.repository = repository
        self.process_buffer = {}
        self.storage_buffer = []

    def parse(self, file_name):
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

    def __valid_line(self, line):
        return 'command' in line and line['command'] in ['statement', 'execute']

    def __process_lines(self):
        for _, lines in self.process_buffer.iteritems():
            if len(lines) > 1:
                line = self.__process_multipart_line(lines)
            else:
                line = lines[0]
            if self.__valid_line(line):
                self.storage_buffer.append(json.dumps(line))
            self.__flush_storage_buffer(1000)
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

class LogDataExtractionService(object):
    """
    Class whose main responsability is to process the parsed log file
    and build the final data with XYZ and affected table names
    """

    def __init__(self, repository):
        self.repository = repository
        self.storage_buffer = []


    def process(self, input_file):
        with open(input_file, 'r+b') as f:
            for line in f:
                query_json = json.loads(line)
                data = self.__filter_query(query_json['query'])
                if data:
                    # Add rest of data and store in file
                    data['timestamp'] = query_json['timestamp']
                    data['duration'] = query_json['duration']
                    data['user'] = query_json['user']
                    data['database'] = query_json['database']
                    self.storage_buffer.append(data)
        self.__flush_storage_buffer()

    def __flush_storage_buffer(self, buffer_limit=0):
        if len(self.storage_buffer) >= buffer_limit:
            self.repository.store(self.storage_buffer)
            self.storage_buffer = []

    def __filter_query(self, query):
        query_stmt = psqlparse.parse(query)[0]
        if isinstance(query_stmt, dict):
            return None
        bbox_pattern = re.compile(r'.*(ST_AsTWKB\(ST_Simplify\(ST_RemoveRepeatedPoints|ST_AsBinary\(ST_Simplify\(ST_SnapToGrid|_zoomed).*(ST_MakeEnvelope\((?P<bbox_env>.*?)\,\d+\)|(ST_MakeEnvelope|BOX3D)\((?P<bbox_3d>.*?)\))', re.IGNORECASE)
        basemaps_pattern = re.compile(r'FROM\s(?P<basemaps_function>(\S+_zoomed|high_road(_labels)?|tunnels|bridges))',re.IGNORECASE)
        bbox_data = bbox_pattern.search(query)
        basemaps_functions = re.findall(basemaps_pattern, query)
        if bbox_data:
            coordinates = self.__get_coordinates_from_bbox_data(bbox_data.groupdict())
            assert len(coordinates) == 4, 'number of coordinates should be 4: xmin, ymin, xmax, ymax'
            xyz = GeoUtils.get_xyz_from_bbox(float(coordinates[0]),
                                             float(coordinates[1]),
                                             float(coordinates[2]),
                                             float(coordinates[3]),
                                             metatile=True)
            if basemaps_functions:
                return {'xyz': xyz, 'tables': basemaps_functions,
                        'basemaps': True, 'update': False}
            else:
                return {'xyz': xyz, 'tables': list(query_stmt.tables()),
                        'basemaps': False, 'update': False}
        elif query_stmt.statement in ['DELETE', 'INSERT', 'UPDATE']:
            return {'bbox': None, 'tables': list(query_stmt.tables()), 'basemaps': False, 'update': True}
        else:
            return None

    def __get_coordinates_from_bbox_data(self, bbox_data):
        if bbox_data['bbox_3d']:
            bbox = []
            list_bbox = bbox_data['bbox_3d'].split(',')
            for part in list_bbox:
                bbox.extend(part.split(' '))
        elif bbox_data['bbox_env']:
            bbox = bbox_data['bbox_env'].split(',')
        return bbox

