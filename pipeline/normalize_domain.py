"""normalize_domain.py -- origin -> registrable domain.

The grain trap: httparchive.crawl.pages is ~one row per origin
(scheme + host, e.g. https://shop.example.com). The business wants one row
per registrable domain (example.com), because that's the join key the sales
database uses and "example.com" is the company, not "shop.example.com".

Naively taking the last two dot-separated labels breaks on multi-part public
suffixes like .co.uk, .com.au, .github.io -- "example.co.uk" would become
"co.uk", silently merging every UK company on that path into one bucket.
This module uses `tldextract`, which ships (and can refresh) the Mozilla
Public Suffix List, so multi-label suffixes are handled correctly.

The domain contract (also documented in docs/design_doc.md, section
"join key contract"):

    domain = the registrable domain (eTLD+1), derived with a
    public-suffix-aware parser, lowercased, with no scheme, path, port,
    query string, or leading "www.".

Examples:
    https://www.example.com/           -> example.com
    https://shop.example.co.uk/product -> example.co.uk
    https://blog.example.com:8443      -> example.com
    http://EXAMPLE.COM                 -> example.com
"""
from __future__ import annotations

from dataclasses import dataclass

import tldextract

# Use tldextract's cached, offline-first PSL snapshot. I explicitly disable
# live HTTP fetches so this pipeline has no runtime dependency on the
# Mozilla PSL endpoint being reachable at 3 AM -- it uses whatever snapshot
# shipped with the package (or was fetched once, in a controlled step during
# environment setup). See README.md "updating the public suffix list".
_extractor = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)


@dataclass(frozen=True)
class NormalizedDomain:
    origin: str            # original root_page as it appeared in HTTP Archive
    registrable_domain: str | None  # eTLD+1, or None if unparseable/invalid
    subdomain: str | None
    is_valid: bool
    reason: str | None = None  # populated when is_valid is False


def normalize_origin(origin: str) -> NormalizedDomain:
    """Convert a single HTTP Archive `root_page` URL into a registrable domain.

    Never raises -- unparseable input comes back as is_valid=False with a
    reason, so a handful of malformed rows in a 10-16M row crawl can't crash
    the whole pipeline. build_snapshot.py routes these to data/rejected/
    instead of silently dropping them.
    """
    if not origin or not isinstance(origin, str):
        return NormalizedDomain(origin=str(origin), registrable_domain=None,
                                 subdomain=None, is_valid=False,
                                 reason="empty_or_non_string_origin")

    stripped = origin.strip()
    if not stripped:
        return NormalizedDomain(origin=origin, registrable_domain=None,
                                 subdomain=None, is_valid=False,
                                 reason="empty_after_strip")

    try:
        ext = _extractor(stripped)
    except Exception as exc:  # tldextract is generally safe, but be defensive
        return NormalizedDomain(origin=origin, registrable_domain=None,
                                 subdomain=None, is_valid=False,
                                 reason=f"tldextract_error:{exc}")

    if not ext.domain or not ext.suffix:
        return NormalizedDomain(origin=origin, registrable_domain=None,
                                 subdomain=None, is_valid=False,
                                 reason="no_registrable_domain_found")

    registrable = f"{ext.domain}.{ext.suffix}".lower()
    subdomain = ext.subdomain.lower() if ext.subdomain else None

    return NormalizedDomain(origin=origin, registrable_domain=registrable,
                             subdomain=subdomain, is_valid=True)


def normalize_batch(origins: list[str]) -> list[NormalizedDomain]:
    return [normalize_origin(o) for o in origins]


if __name__ == "__main__":
    samples = [
        "https://www.example.com/",
        "https://shop.example.co.uk/product",
        "https://blog.example.com:8443",
        "http://EXAMPLE.COM",
        "https://sub.sub.github.io",
        "not a url",
        "",
    ]
    for s in samples:
        print(s, "->", normalize_origin(s))
