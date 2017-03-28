class InMemoryRepository(object):
    def __init__(self):
        self.storage = []

    def store(self, data):
        self.storage.extend(data)
    
    def get_all(self):
        return self.storage