import pymongo
import unittest


def throw_if_mongodb_is_unavailable(host, port):
    import socket
    sock = None
    try:
        sock = socket.create_connection(
            (host, port),
            timeout=1)  # one second
    except socket.error as err:
        raise EnvironmentError(
            "Can't connect to MongoDB at {host}:{port} because: {err}"
                .format(**locals()))
    finally:
        if sock is not None:
            sock.close()


class TestClass(unittest.TestCase):
    def test_mongo_connection(self):
        HOST = 'localhost'
        PORT = 27017
        throw_if_mongodb_is_unavailable(HOST, PORT)
        conn = pymongo.MongoClient(HOST, PORT)
        assert conn.admin.command('ismaster')['ok'] == 1.0
