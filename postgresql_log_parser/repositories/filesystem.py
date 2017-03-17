class FileRepository(object):

    def __init__(self, file_name=None):
        self.file_name = file_name or '/tmp/postgresql_parser.log'

    def store(self, data):
        with open(self.file_name, 'a') as f:
            f.write('\n'.join(data) + '\n')
