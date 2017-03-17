import pyparsing as pyp
import re
from itertools import izip

class PyParsingParser(object):

    def __init__(self, filter_pattern=None):
        header = self.__header_pattern()
        self.regex_pattern = None
        if filter_pattern:
            self.regex_pattern = re.compile(filter_pattern, re.I)
        self.pattern = pyp.Or([header + \
            pyp.Or([self.__body_query_pattern(),
                    self.__body_multipart_pattern(),
                    self.__discard_pattern()]), self.__discard_pattern()])

    def parse_line(self,line):
        try:
            if self.regex_pattern and self.regex_pattern.search(line):
                return {}
            else:
                data = self.pattern.parseString(line)
                return self.__to_dict(data)
        except pyp.ParseException as parse_e:
            print parse_e
            return []

    def __header_pattern(self):
        timestamp = pyp.Combine(pyp.Word(pyp.alphas) + pyp.White() + pyp.Word(pyp.nums) +
                                pyp.White() + pyp.Word(pyp.nums+':'))
        hostname = pyp.Word(pyp.alphanums+'-')
        pid = pyp.Suppress('postgres') + pyp.QuotedString('[', endQuoteChar=']') + pyp.Suppress(':' + pyp.White())
        return timestamp + hostname + pid

    def __body_query_pattern(self):
        pid_part = pyp.Suppress('[') + pyp.Word(pyp.nums) + pyp.Suppress(
            '-') + pyp.Word(pyp.nums) + pyp.Suppress(']' + pyp.White())
        timestamp = pyp.Combine(pyp.Word(pyp.nums + '-') + pyp.White() +
                                pyp.Word(pyp.nums + ':') + pyp.White() +
                                pyp.Word(pyp.alphas)) + pyp.Suppress(pyp.White())
        user = pyp.Optional('[') + pyp.Word(pyp.alphanums + '_' + '-') + \
            pyp.Optional(']')
        dbname = pyp.Optional('[') + pyp.Word(pyp.alphanums + '_' + '-') + \
            pyp.Optional(']')
        duration = pyp.Suppress(pyp.Word(pyp.alphanums + '[' + ']') + pyp.White() + pyp.Word(pyp.alphas) + ':' + pyp.White(
        ) + 'duration:' + pyp.White()) + pyp.Word(pyp.nums + '.') + pyp.Suppress(pyp.White() + 'ms' + pyp.White())
        type_command = pyp.Word(pyp.alphas) + pyp.Suppress(pyp.Optional(
            pyp.White() + pyp.Word(pyp.alphas + '<' + '>')) + ':' + pyp.White())
        query = pyp.restOfLine()
        return pid_part + timestamp + user + dbname + duration + type_command + query

    def __body_multipart_pattern(self):
        return pyp.Or([self.__body_multipart_query_pattern(),
                       self.__body_multipart_parameters_pattern()])

    def __body_multipart_query_pattern(self):
        pid_part = pyp.Suppress('[') + pyp.Word(pyp.nums) + pyp.Suppress(
            '-') + pyp.Word(pyp.nums) + pyp.Suppress(']' + pyp.White())
        query_part = pyp.Suppress('#011' + pyp.Optional(pyp.White())) + pyp.restOfLine()
        return pid_part + query_part

    def __body_multipart_parameters_pattern(self):
        pid_part = pyp.Suppress('[') + pyp.Word(pyp.nums) + pyp.Suppress(
            '-') + pyp.Word(pyp.nums) + pyp.Suppress(']' + pyp.White())
        timestamp = pyp.Combine(pyp.Word(pyp.nums + '-') + pyp.White() +
                                pyp.Word(pyp.nums + ':') + pyp.White() +
                                pyp.Word(pyp.alphas)) + pyp.Suppress(pyp.White())
        user = pyp.Optional('[') + pyp.Word(pyp.alphanums + '_' + '-') + \
            pyp.Optional(']')
        dbname = pyp.Optional('[') + pyp.Word(pyp.alphanums + '_' + '-') + \
            pyp.Optional(']')
        parameters = pyp.Suppress('[' + pyp.Word(pyp.alphanums) + ']' + pyp.White() +
                                  pyp.Word(pyp.alphas) + ':' + pyp.White() + 'parameters:' + pyp.White()) + pyp.restOfLine()
        return pid_part + timestamp + user + dbname + parameters

    def __discard_pattern(self):
        return pyp.restOfLine()

    def __parse_parameters(self, data):
        param_pattern = pyp.delimitedList(pyp.OneOrMore(pyp.Word(
            '$' + pyp.nums) + pyp.Suppress(pyp.White() + '=' + pyp.White()) + pyp.QuotedString('\'')), delim=",")
        parsed_parameters = param_pattern.parseString(data)
        iterator = iter(parsed_parameters)
        parameters_dict = dict(izip(iterator, iterator))
        return parameters_dict

    def __to_dict(self, data):
        parsed_data = {}
        # TODO should be a better way to indentify the type of parse
        if len(data) == 11:
            parsed_data = {'timestamp': data[5], 'user': data[6], 'database': data[7],
                           'pid': data[2], 'pid_part': data[3], 'part': data[4], 
                           'duration': data[8], 'command': data[9], 'query': data[10],
                           'multipart': False}
        elif len(data) == 9:
            parameters_list = self.__parse_parameters(data[8])
            parsed_data = {'timestamp': data[5], 'user': data[6], 'database': data[7],
                           'pid': data[2], 'pid_part': data[3], 'part': data[4], 
                           'parameters': parameters_list, 'multipart': True}
        elif len(data) == 6:
            parsed_data = {'timestamp': data[0], 'pid': data[2], 'pid_part': data[3],
                           'part': data[4], 'query': data[5], 'multipart': True}
        elif len(data) <= 4:
            parsed_data = {}

        return parsed_data
