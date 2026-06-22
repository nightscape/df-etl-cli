"""URI parsing helpers.

Port of ``core/.../UriHelpers.scala``. A dfio URI looks like::

    name+scheme://host/path?k=v          # source / sink   (name optional)
    source+sink+scheme://...             # transformer     (names optional)

The leading ``name+`` / ``source+sink+`` segments live in the *scheme* part of
the URI (before ``://``), so we split the scheme on ``+`` ourselves.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import unquote, urlsplit


@dataclass(frozen=True)
class ParsedUri:
    raw: str
    scheme: str
    host: str | None
    port: int | None
    path: str
    query: str | None

    @classmethod
    def parse(cls, uri: str) -> "ParsedUri":
        parts = urlsplit(uri)
        return cls(
            raw=uri,
            scheme=parts.scheme,
            host=parts.hostname,
            port=parts.port,
            path=unquote(parts.path),
            query=parts.query or None,
        )

    @property
    def query_params(self) -> dict[str, str]:
        if not self.query:
            return {}
        out: dict[str, str] = {}
        for pair in self.query.split("&"):
            if not pair:
                continue
            key, _, value = pair.partition("=")
            out[key] = unquote(value)
        return out

    @property
    def path_parts(self) -> list[str]:
        return [p for p in self.path.split("/") if p]

    def scheme_and_name(self) -> tuple[str, str | None]:
        """``name+scheme`` -> (scheme, name); ``scheme`` -> (scheme, None)."""
        segments = self.scheme.split("+")
        if len(segments) == 2:
            name, scheme = segments
            return scheme, (name or None)
        if len(segments) == 1:
            return segments[0], None
        raise ValueError(f"Cannot parse name from scheme {self.scheme!r}")

    def scheme_source_sink(self) -> tuple[str, str | None, str | None]:
        """``source+sink+scheme`` / ``source+scheme`` / ``scheme``."""
        segments = self.scheme.split("+")
        if len(segments) == 3:
            source, sink, scheme = segments
            return scheme, (source or None), (sink or None)
        if len(segments) == 2:
            source, scheme = segments
            return scheme, (source or None), None
        if len(segments) == 1:
            return segments[0], None, None
        raise ValueError(f"Cannot parse source/sink from scheme {self.scheme!r}")
