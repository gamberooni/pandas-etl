from abc import ABC, abstractmethod


class WriterBase(ABC):
    @abstractmethod
    def write(self):
        pass
