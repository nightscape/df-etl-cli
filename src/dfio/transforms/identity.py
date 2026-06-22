"""identity:// — passthrough (used to rename a DataFrame). Ports IdentityTransformerParser."""

from __future__ import annotations

from ..base import Transformer, TransformerParser
from ..uri import ParsedUri


class IdentityParser(TransformerParser):
    @property
    def schemes(self) -> list[str]:
        return ["identity"]

    def build(self, uri: ParsedUri) -> Transformer:
        return lambda table: table
