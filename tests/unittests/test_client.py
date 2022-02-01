import pytest
from tap_sailthru.client import SailthruClient

@pytest.fixture
def client():
    return SailthruClient('test', 'test', 'test', 300)

@pytest.mark.parametrize('test_input, expected', [
    ([{'a': 1}], [1]),
    ([{'key1': 'test1'}, {'key2': {'key3': 'test2'}}], ['test1', 'test2']),
    ({'names': ['alice', 'sally', 'john']}, ['alice', 'sally', 'john']),
    ({'a': ['one', 'two'], 'b': {'c': 'three'}}, ['one', 'two', 'three']),])
def test_extract_params(client, test_input, expected):
    assert client.extract_params(test_input) == expected

@pytest.mark.parametrize('secret, test_input, expected', [
    ('secret', ['one', 'two', 'three'], b'secretonethreetwo'),
    ('password', {'a': ['test1', 'test2'], 'b': {'c': 'test3'}}, b'passwordtest1test2test3')])
def test_get_signature_string(client, secret, test_input, expected):
    assert client.get_signature_string(test_input, secret) == expected

@pytest.mark.parametrize('secret, test_input, expected', [
    ('secret', ['one', 'two', 'three'], '764794f03e1e345c32d4c81aae2815eb'),
    ('password', {'a': ['test1', 'test2'], 'b': {'c': 'test3'}}, 'adde49d639598daa7ba79af7c2fff8f9')])
def test_get_signature_hash(client, secret, test_input, expected):
    assert client.get_signature_hash(test_input, secret) == expected
