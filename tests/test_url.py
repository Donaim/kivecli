import pytest
from kivecli.url import URL
from kivecli.usererror import UserError


def test_invalid_url():
    with pytest.raises(UserError):
        URL("not a url")


def test_valid_url():
    url = URL("http://example.com")
    assert str(url) == "<http://example.com>"
