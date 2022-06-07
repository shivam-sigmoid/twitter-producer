import pytest
import sys

sys.path.append("../")
from server.app import app

books = [
    {
        "id": 1,
        "title": "CS50",
        "description": "Intro to CS and art of programming!",
        "author": "Havard",
        "borrowed": False
    },
    {
        "id": 2,
        "title": "Python 101",
        "description": "little python code book.",
        "author": "Will",
        "borrowed": False
    }
]


@pytest.fixture
def client():
    return app.test_client()


def test_index_route():
    response = app.test_client().get('/')
    assert response.status_code == 200


def test_service_bad_http_method(client):
    resp = client.get('/service')
    assert resp.status_code == 404


def test_service_no_json_body(client):
    resp = client.post('/service', data='something')
    assert resp.status_code == 404


def test_service_missing_email(client):
    resp = client.post('/service', json={'username': 'mehdi'})
    assert resp.status_code == 404


@app.route("/bookapi/books")
def get_books():
    """ function to get all books """
    # check this route too
    # return jsonify({"Books": books})
    return True


def test_get_all_books():
    response = app.test_client().get('/bookapi/books')
    assert response.status_code == 500


#        script: |
#          kill $(pgrep -f flask)

# import unittest
#
#
# class TestClass(unittest.TestCase):
#     @pytest.fixture
#     def client(self):
#         return app.test_client()
#
#     def test_index_route(self):
#         response = app.test_client().get('/')
#         assert response.status_code == 200
#
#     def test_get_tweets_geo_enabled(self):
#         resp = app.test_client().get('/get_tweets_geo_enabled')
#         assert resp.status_code == 200
#
#     def test_get_tweets(self):
#         resp = app.test_client().get('/get_tweets')
#         assert resp.status_code == 200
#
#     def test_get_tweets_per_country(self):
#         resp = app.test_client().get('/get_tweets_per_country')
#         assert resp.status_code == 200
#
#     def test_get_tweets_per_country_per_day(self):
#         resp = app.test_client().get('/get_tweets_per_country_per_day')
#         assert resp.status_code == 200
#
#     def test_get_precautionary_measures(self):
#         resp = app.test_client().get('/get_precautionary_measures')
#         assert resp.status_code == 200
#
#     def test_get_most_occured_words(self):
#         resp = app.test_client().get('/get_most_occured_words')
#         assert resp.status_code == 200
#
#     def test_get_donations_who_data(self):
#         resp = app.test_client().get('/get_donations_who_data')
#         assert resp.status_code == 200
#
#     def test_get_donations_funding(self):
#         resp = app.test_client().get('/get_donations_funding')
#         assert resp.status_code == 200
#
#     def test_get_impacted_country(self):
#         resp = app.test_client().get('/get_impacted_country')
#         assert resp.status_code == 200
