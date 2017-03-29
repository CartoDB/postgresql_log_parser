from abc import ABCMeta, abstractmethod

class Repository(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def store(self, data): raise NotImplementedError