import os
import tempfile

import pytest

# 必须在导入 app 之前指定临时数据库
_TMP_DIR = tempfile.mkdtemp(prefix="robot_data_test_")
_DB_PATH = os.path.join(_TMP_DIR, "test_robot_data.db")
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"

from fastapi.testclient import TestClient  # noqa: E402

from app.database import Base, engine  # noqa: E402
from main import app  # noqa: E402


@pytest.fixture(autouse=True)
def reset_database():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield


class ApiClient:
    """让 TestClient 的 get/post/delete 返回 (status_code, json_body)。"""

    def __init__(self, client):
        self._client = client

    def request(self, method, path, json=None, params=None):
        resp = self._client.request(method, path, json=json, params=params)
        try:
            return resp.status_code, resp.json()
        except Exception:
            return resp.status_code, resp.text

    def get(self, path, params=None):
        return self.request("GET", path, params=params)

    def post(self, path, json=None):
        return self.request("POST", path, json=json)

    def delete(self, path, json=None):
        return self.request("DELETE", path, json=json)

    @property
    def raise_server_exceptions(self):
        return self._client.raise_server_exceptions

    @raise_server_exceptions.setter
    def raise_server_exceptions(self, value):
        self._client.raise_server_exceptions = value


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield ApiClient(c)


@pytest.fixture
def db_path():
    return _DB_PATH
