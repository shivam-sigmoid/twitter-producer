import pytest
from server.app import app
import unittest


class TestClass(unittest.TestCase):
    @pytest.fixture
    def client(self):
        return app.test_client()

    def test_index_route(self):
        response = app.test_client().get('/')
        assert response.status_code == 200

    def test_get_tweets_geo_enabled(self):
        resp = app.test_client().get('/get_tweets_geo_enabled')
        assert resp.status_code == 200

    def test_get_tweets(self):
        resp = app.test_client().get('/get_tweets')
        assert resp.status_code == 200

    def test_task_1(self):
        resp = app.test_client().get('/task_1')
        assert resp.status_code == 200

    def test_task_2(self):
        resp = app.test_client().get('/task_2')
        assert resp.status_code == 200

    def test_task_5(self):
        resp = app.test_client().get('/task_5')
        assert resp.status_code == 200

    def test_task_7(self):
        resp = app.test_client().get('/task_7')
        assert resp.status_code == 200
