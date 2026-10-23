"""Text normalisation shared by both sources.

Street names are the only join key between closures and businesses (the business
layer carries no street code), so both sides must be normalised identically.
"""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# Hebrew gershayim / geresh and typographic quotes -> ASCII equivalents
_QUOTE_FROM = "״׳“”‘’`"
_QUOTE_TO = "\"'\"\"'''"


def clean_text(c: Column) -> Column:
    """Unify quote characters, collapse whitespace, trim. Empty -> NULL."""
    s = F.translate(c.cast("string"), _QUOTE_FROM, _QUOTE_TO)
    s = F.trim(F.regexp_replace(s, r"\s+", " "))
    return F.when(s == "", None).otherwise(s)


def street_key(c: Column) -> Column:
    """Join key for street names. The closures file encodes 'no street' as 0."""
    s = clean_text(c)
    return F.when(s.isNull() | (s == "0"), None).otherwise(s)


def strip_wrapping_quotes(c: Column) -> Column:
    """Some category values arrive wrapped in a stray pair of quotes, e.g. '"משרד השמה וכ"א"'."""
    s = clean_text(c)
    return F.when(s.rlike(r'^".+"$'), F.regexp_replace(s, r'^"(.*)"$', "$1")).otherwise(s)
