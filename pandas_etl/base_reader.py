from abc import ABC, abstractmethod


class ReaderBase(ABC):
    @abstractmethod
    def read(self):
        pass
