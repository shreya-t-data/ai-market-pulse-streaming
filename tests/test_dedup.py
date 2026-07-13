import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "producers"))

from news_producer import is_duplicate


def test_new_url_is_not_duplicate():
    seen = set()
    assert is_duplicate("https://example.com/article1", seen) is False


def test_repeated_url_is_duplicate():
    seen = set()
    is_duplicate("https://example.com/article1", seen)
    assert is_duplicate("https://example.com/article1", seen) is True


def test_empty_url_treated_as_duplicate():
    seen = set()
    assert is_duplicate(None, seen) is True
    assert is_duplicate("", seen) is True