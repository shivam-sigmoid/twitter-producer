import pytest
# from server.app import app

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


@app.route("/bookapi/books")
def get_books():
    """ function to get all books """
    return jsonify({"Books": books})


@pytest.fixture
def client():
    return app.test_client()


def test_index_route():
    response = app.test_client().get('/')
    assert response.status_code == 404


def test_service(client):
    resp = client.post('/service', json={'email_address': 'some@thing.com', 'username': 'mehdi'})
    assert resp.status_code == 404


def test_service_bad_http_method(client):
    resp = client.get('/service')
    assert resp.status_code == 404


def test_service_no_json_body(client):
    resp = client.post('/service', data='something')
    assert resp.status_code == 404


def test_service_missing_email(client):
    resp = client.post('/service', json={'username': 'mehdi'})
    assert resp.status_code == 404


def test_get_all_books():
    response = app.test_client().get('/bookapi/books')
    assert response.status_code == 500
