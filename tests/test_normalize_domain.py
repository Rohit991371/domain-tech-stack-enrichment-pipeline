import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.normalize_domain import normalize_origin


def test_strips_scheme_and_www():
    r = normalize_origin("https://www.example.com/")
    assert r.is_valid
    assert r.registrable_domain == "example.com"


def test_multi_label_public_suffix_not_collapsed():
    """The core grain-trap regression test: naive 'last two labels' logic
    would turn example.co.uk into 'co.uk', silently merging every UK
    business on a .co.uk domain. This must not happen."""
    r = normalize_origin("https://shop.example.co.uk/product")
    assert r.is_valid
    assert r.registrable_domain == "example.co.uk"
    assert r.registrable_domain != "co.uk"


def test_strips_port_and_lowercases():
    r = normalize_origin("https://blog.EXAMPLE.com:8443")
    assert r.registrable_domain == "example.com"


def test_different_subdomains_map_to_same_domain():
    a = normalize_origin("https://www.example.com/")
    b = normalize_origin("https://shop.example.com/")
    c = normalize_origin("https://example.com/")
    assert a.registrable_domain == b.registrable_domain == c.registrable_domain == "example.com"


def test_invalid_input_does_not_raise():
    r = normalize_origin("not a url")
    assert not r.is_valid
    assert r.reason is not None


def test_empty_string_does_not_raise():
    r = normalize_origin("")
    assert not r.is_valid


def test_none_like_input_does_not_raise():
    r = normalize_origin(None)  # type: ignore[arg-type]
    assert not r.is_valid
