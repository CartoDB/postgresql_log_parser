from abc import ABCMeta, abstractmethod

class Repository(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def store(self, data): raise NotImplementedError

    @abstractmethod
    def get_all(self): raise NotImplementedError