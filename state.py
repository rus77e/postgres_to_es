import abc
import json
from typing import Any, Optional


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        pass


class JsonFileStorage(BaseStorage):
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
    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self.state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        if key:
            self.state[key] = value
            self.storage.save_state(self.state)

    def get_state(self, key: str, default=None) -> Any:
        if key:
            return self.state.get(key, default)
        else:
            return default
