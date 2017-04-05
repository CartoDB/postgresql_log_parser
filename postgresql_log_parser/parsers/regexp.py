import re

class RegexpParser(object):

    def __init__(self):
        self.pattern = re.compile(r'((?=[a-z]+\s+\d+\s\d+\:\d+\:\d+\s[a-z0-9-]+)((?P<date>[a-z]+\s+\d+\s\d+\:\d+\:\d+)\s(?P<host>[a-z0-9-]+)\s((?=\D+\[\d+\])\D+\[(?P<pid>\d+)\]\:|\D+\:))\s(\[(?P<pid_part>\d+)-(?P<part>\d+)\]\s((?=#011)(#011)+(\s+)?(?P<multipart_query>.*)|((?P<timestamp>\d+-\d+-\d+\s\d+\:\d+\:\d+)\s\w+\s(?P<user>[a-z0-9-_\[\]]+)\s(?P<database>[a-z0-9-_\]\[]+))\s(?P<program>[a-z0-9-_\.\[\]]+).+((?=duration\:\s)duration\:\s(?P<duration>[0-9\.]+)\sms\s+(?P<command>[a-z]+)(\s[a-z\<\>]+)?\:(?=\s)(?P<query>.+)?|(?=parameters\:\s)parameters\:\s+(?P<parameters>.*))|(?P<multipart_query_rest>.*)))|.*)', re.IGNORECASE)

    def parse_line(self, line):
        data = self.pattern.match(line)
        if data:
            grouped_data = data.groupdict()
            if grouped_data['duration']:
                return self.__query_to_dict(grouped_data)
            elif grouped_data['parameters']:
                return self.__parameters_to_dict(grouped_data)
            elif grouped_data['part'] and int(grouped_data['part']) > 1 and (grouped_data['multipart_query'] or grouped_data['multipart_query_rest']):
                return self.__multipart_query_to_dict(grouped_data)
            else:
                return {}
        else:
            return {}

    def __query_to_dict(self, data):
        return {'timestamp': data['timestamp'], 'user': data['user'],
                'database': data['database'], 'pid': data['pid'],
                'pid_part': data['pid_part'], 'part': data['part'],
                'duration': data['duration'], 'command': data['command'],
                'query': data['query'].strip() if data['query'] else '', 
                'host': data['host'], 'date': data['date'],
                'multipart': False}
    def __parameters_to_dict(self, data):
        parameters = self.__parse_parameters(data['parameters'])
        return {'timestamp': data['timestamp'], 'user': data['user'],
                'database': data['database'], 'pid': data['pid'],
                'pid_part': data['pid_part'], 'part': data['part'],
                'parameters': parameters, 'host': data['host'], 'date': data['date'],
                'multipart': True}
    def __parse_parameters(self, str_parameters):
        pattern = re.compile(r'(?P<param_key>\$\d+)\s+\=\s+(?P<param_value>\S+)\,?', re.IGNORECASE)
        parameters = {}
        for match in pattern.finditer(str_parameters):
            data = match.groupdict()
            data['param_value'] = data['param_value'].replace(',', '')
            parameters[data['param_key']] = data['param_value']
        return parameters

    def __multipart_query_to_dict(self, data):
        if data['multipart_query_rest']:
            query = data['multipart_query_rest'].strip()
        elif data['multipart_query']:
            query = data['multipart_query'].strip()
        return {'timestamp': data['timestamp'], 'user': data['user'],
                'database': data['database'], 'pid': data['pid'],
                'pid_part': data['pid_part'], 'part': data['part'],
                'query': query, 'host': data['host'],
                'date': data['date'], 'multipart': True}
