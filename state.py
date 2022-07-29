import abc
import json
import threading
from typing import Any, Optional

lock = threading.Lock()


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        pass


class JsonFileStorage(BaseStorage):
    """
    Storage of data as json in file.
    """
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        with open(self.file_path, 'w') as file:
            file.write(json.dumps(state))

    def retrieve_state(self) -> dict:
        try:
            with open(self.file_path, 'r') as file:
                return json.loads(file.read())
        except OSError:
            return dict()


class State:
    """
    Provide functionality to get/set key-value pairs from/to storage.
    """
    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self.state = self.storage.retrieve_state()

    def set(self, key: str, value: Any) -> None:
        if key:
            with lock:
                self.state[key] = value
                self.storage.save_state(self.state)

    def get(self, key: str, default=None) -> Any:
        if key:
            with lock:
                return self.state.get(key, default)
        else:
            return default
