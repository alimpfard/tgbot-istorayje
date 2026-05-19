import re
from collections import Counter

try:
    from nltk.corpus import stopwords as _sw

    STOPWORDS = set(_sw.words("english"))
except Exception:
    STOPWORDS = {
        "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has",
        "he", "in", "is", "it", "its", "of", "on", "that", "the", "to", "was",
        "were", "will", "with", "this", "but", "they", "have", "had", "what",
        "when", "where", "who", "which", "i", "we", "you", "your", "or", "if",
    }

_WORD_RE = re.compile(r"[A-Za-z][A-Za-z\-']+")


def _tokenize(text):
    return [t.lower() for t in _WORD_RE.findall(text)]


def _keepable(t):
    return len(t) > 2 and t not in STOPWORDS


def tagify(docs):
    """Extract candidate tags from a list of short text documents.

    Returns a list of [tag, score] pairs with scores in 0-100.
    Higher score = more representative across the corpus.
    """
    if not docs:
        return []

    counts = Counter()
    for doc in docs:
        if not isinstance(doc, str) or not doc.strip():
            continue
        tokens = _tokenize(doc)
        kept = [t for t in tokens if _keepable(t)]
        for t in kept:
            counts[t] += 1
        for a, b in zip(kept, kept[1:]):
            counts[f"{a}_{b}"] += 2
        joined = "_".join(kept)
        if joined and joined not in counts:
            counts[joined] += 1

    if not counts:
        return []

    top = max(counts.values())
    out = [[term, round(c / top * 100, 2)] for term, c in counts.items()]
    out.sort(key=lambda x: -x[1])
    return out
