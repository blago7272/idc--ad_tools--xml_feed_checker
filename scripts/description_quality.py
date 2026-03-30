#!/usr/bin/env python3
"""Lightweight description scoring with optional NLP enrichments."""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from html import unescape
from statistics import mean
from typing import Dict, Iterable, List, Optional

TOKEN_RE = re.compile(r"[0-9A-Za-zА-Яа-я]+(?:['’-][0-9A-Za-zА-Яа-я]+)?", re.UNICODE)
HTML_RE = re.compile(r"<[^>]+>")
URL_RE = re.compile(r"(?:https?://|www\.)", re.IGNORECASE)
MEASUREMENT_RE = re.compile(
    r"\b\d+(?:[.,]\d+)?\s?(?:cm|mm|m|kg|g|x|х|sm|cm\.|mm\.|kg\.|g\.|%|см|мм|кг|гр)\b",
    re.IGNORECASE,
)
WHITESPACE_RE = re.compile(r"\s+")

LANGUA_LANGUAGE_MAP = {
    "bg": "BULGARIAN",
    "de": "GERMAN",
    "en": "ENGLISH",
    "es": "SPANISH",
    "fr": "FRENCH",
    "it": "ITALIAN",
    "mk": "MACEDONIAN",
    "ro": "ROMANIAN",
    "ru": "RUSSIAN",
    "tr": "TURKISH"
}


@dataclass(frozen=True)
class AspectRule:
    aspect_id: str
    keywords: tuple[str, ...]
    patterns: tuple[re.Pattern[str], ...]

    def match(self, clean_text: str, lowered_text: str) -> Dict[str, object]:
        keyword_hits = [keyword for keyword in self.keywords if keyword.lower() in lowered_text]
        pattern_hits = [pattern.pattern for pattern in self.patterns if pattern.search(clean_text)]
        return {
            "aspect_id": self.aspect_id,
            "covered": bool(keyword_hits or pattern_hits),
            "keyword_hits": keyword_hits[:3],
            "pattern_hits": pattern_hits[:3],
        }


def _try_import(module_name: str):
    try:
        return __import__(module_name)
    except ImportError:
        return None


def available_backends() -> Dict[str, bool]:
    return {
        "simplemma": _try_import("simplemma") is not None,
        "wordfreq": _try_import("wordfreq") is not None,
        "lingua": _try_import("lingua") is not None,
    }


def _normalize_whitespace(text: str) -> str:
    return WHITESPACE_RE.sub(" ", text.strip())


def _tokenize(text: str) -> List[str]:
    return TOKEN_RE.findall(text)


def _alpha_tokens(tokens: Iterable[str]) -> List[str]:
    return [token for token in tokens if any(char.isalpha() for char in token)]


def _safe_ratio(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return numerator / denominator


@lru_cache(maxsize=16)
def _lingua_detector(codes: tuple[str, ...]):
    lingua = _try_import("lingua")
    if lingua is None:
        return None

    members = []
    for code in codes:
        enum_name = LANGUA_LANGUAGE_MAP.get(code.lower())
        if not enum_name:
            continue
        member = getattr(lingua.Language, enum_name, None)
        if member is not None:
            members.append(member)

    if len(members) < 2:
        return None

    return lingua.LanguageDetectorBuilder.from_languages(*members).build()


class DescriptionAnalyzer:
    def __init__(self, config: Dict[str, object], aspect_groups: Optional[Dict[str, object]] = None):
        self.config = config
        self.simplemma = _try_import("simplemma")
        self.wordfreq = _try_import("wordfreq")
        self.lingua = _try_import("lingua")
        self.required_aspect_warn_ratio = float(config.get("required_aspect_warn_ratio", 0.67))
        self.aspect_groups = self._compile_aspect_groups(aspect_groups or {})
        self.aspect_group_lookup = {group_name.casefold(): group_name for group_name in self.aspect_groups}

    def backend_status(self) -> Dict[str, bool]:
        return available_backends()

    def _compile_aspect_groups(self, raw_groups: Dict[str, object]) -> Dict[str, Dict[str, List[AspectRule]]]:
        compiled: Dict[str, Dict[str, List[AspectRule]]] = {}
        for group_name, group in raw_groups.items():
            compiled[group_name] = {"required_aspects": [], "optional_aspects": []}
            for bucket in ("required_aspects", "optional_aspects"):
                rules: List[AspectRule] = []
                for item in group.get(bucket, []):
                    rules.append(
                        AspectRule(
                            aspect_id=item["id"],
                            keywords=tuple(item.get("keywords", [])),
                            patterns=tuple(re.compile(pattern, re.IGNORECASE) for pattern in item.get("patterns", [])),
                        )
                    )
                compiled[group_name][bucket] = rules
        return compiled

    def _pick_aspect_group(self, product_type: str) -> Optional[str]:
        if not self.aspect_groups:
            return None
        if product_type in self.aspect_groups:
            return product_type
        normalized = product_type.casefold().strip()
        if normalized in self.aspect_group_lookup:
            return self.aspect_group_lookup[normalized]
        if "generic" in self.aspect_groups:
            return "generic"
        return None

    def _analyze_aspects(self, clean_text: str, product_type: str) -> Dict[str, object]:
        group_name = self._pick_aspect_group(product_type)
        if not group_name:
            return {
                "group_name": None,
                "required_ratio": None,
                "optional_ratio": None,
                "coverage_band": None,
                "required_details": [],
                "optional_details": [],
                "missing_required_aspects": [],
            }

        lowered_text = clean_text.lower()
        group = self.aspect_groups[group_name]
        required_details = [rule.match(clean_text, lowered_text) for rule in group["required_aspects"]]
        optional_details = [rule.match(clean_text, lowered_text) for rule in group["optional_aspects"]]

        required_ratio = _safe_ratio(sum(1 for item in required_details if item["covered"]), len(required_details)) if required_details else 1.0
        optional_ratio = (
            _safe_ratio(sum(1 for item in optional_details if item["covered"]), len(optional_details))
            if optional_details
            else None
        )

        if required_ratio >= 0.99:
            coverage_band = "strong"
        elif required_ratio >= self.required_aspect_warn_ratio:
            coverage_band = "partial"
        else:
            coverage_band = "weak"

        return {
            "group_name": group_name,
            "required_ratio": required_ratio,
            "optional_ratio": optional_ratio,
            "coverage_band": coverage_band,
            "required_details": required_details,
            "optional_details": optional_details,
            "missing_required_aspects": [item["aspect_id"] for item in required_details if not item["covered"]],
        }

    def analyze(self, text: str, title: str = "", product_type: str = "") -> Dict[str, object]:
        raw_text = text or ""
        clean_text = _normalize_whitespace(unescape(raw_text))
        tokens = _tokenize(clean_text)
        alpha_tokens = _alpha_tokens(tokens)
        lowered_alpha_tokens = [token.lower() for token in alpha_tokens]
        unique_tokens = set(lowered_alpha_tokens)

        letters = [char for char in clean_text if char.isalpha()]
        uppercase_letters = [char for char in letters if char.isupper()]
        all_caps_ratio = _safe_ratio(len(uppercase_letters), len(letters))

        has_html = bool(HTML_RE.search(clean_text))
        has_url = bool(URL_RE.search(clean_text))
        measurement_hits = len(MEASUREMENT_RE.findall(clean_text))
        sentence_count = sum(1 for part in re.split(r"[.!?]+", clean_text) if part.strip())
        char_count = len(clean_text)
        token_count = len(alpha_tokens)
        unique_token_ratio = _safe_ratio(len(unique_tokens), token_count)

        title_tokens = {token.lower() for token in _alpha_tokens(_tokenize(title)) if len(token) >= 4}
        description_token_set = {token.lower() for token in alpha_tokens if len(token) >= 4}
        title_overlap_ratio = _safe_ratio(len(title_tokens & description_token_set), len(title_tokens))
        aspect_result = self._analyze_aspects(clean_text, product_type) if clean_text else {
            "group_name": self._pick_aspect_group(product_type),
            "required_ratio": 0.0 if self._pick_aspect_group(product_type) else None,
            "optional_ratio": None,
            "coverage_band": "weak" if self._pick_aspect_group(product_type) else None,
            "required_details": [],
            "optional_details": [],
            "missing_required_aspects": [],
        }

        issues: List[str] = []
        score = 100

        warn_short_chars = int(self.config.get("warn_short_chars", 80))
        recommended_min_chars = int(self.config.get("recommended_min_chars", 150))
        soft_long_chars = int(self.config.get("soft_long_chars", 1500))
        warn_all_caps_ratio = float(self.config.get("warn_all_caps_ratio", 0.45))
        warn_low_unique_token_ratio = float(self.config.get("warn_low_unique_token_ratio", 0.45))
        warn_title_overlap_ratio = float(self.config.get("warn_title_overlap_ratio", 0.85))
        warn_min_sentences = int(self.config.get("warn_min_sentences", 1))

        if not clean_text:
            issues.append("empty_description")
            score = 0
        else:
            if char_count < warn_short_chars:
                issues.append("description_too_short")
                score -= 25
            elif char_count < recommended_min_chars:
                issues.append("description_thin")
                score -= 10

            if char_count > soft_long_chars:
                issues.append("description_very_long")
                score -= 5

            if has_html:
                issues.append("contains_html")
                score -= 15

            if has_url:
                issues.append("contains_url")
                score -= 15

            if all_caps_ratio >= warn_all_caps_ratio:
                issues.append("mostly_all_caps")
                score -= 15

            if sentence_count <= warn_min_sentences and char_count >= recommended_min_chars:
                issues.append("low_sentence_structure")
                score -= 5

            if token_count and unique_token_ratio < warn_low_unique_token_ratio:
                issues.append("low_unique_token_ratio")
                score -= 10

            if title_tokens and title_overlap_ratio >= warn_title_overlap_ratio and char_count < 220:
                issues.append("too_similar_to_title")
                score -= 10

            if measurement_hits == 0 and char_count >= recommended_min_chars:
                issues.append("no_measurement_or_numeric_detail")
                score -= 3

            required_ratio = aspect_result["required_ratio"]
            if required_ratio is not None and aspect_result["missing_required_aspects"]:
                for aspect_id in aspect_result["missing_required_aspects"]:
                    issues.append(f"missing_aspect:{aspect_id}")
                score -= 6 * len(aspect_result["missing_required_aspects"])
                if required_ratio < self.required_aspect_warn_ratio:
                    issues.append("low_required_aspect_coverage")
                    score -= 8

        enrichments: Dict[str, object] = {}
        if aspect_result["group_name"]:
            enrichments["aspect_group"] = aspect_result["group_name"]
            enrichments["aspect_coverage_band"] = aspect_result["coverage_band"]
            enrichments["required_aspect_coverage"] = round(float(aspect_result["required_ratio"]), 4) if aspect_result["required_ratio"] is not None else None
            enrichments["optional_aspect_coverage"] = round(float(aspect_result["optional_ratio"]), 4) if aspect_result["optional_ratio"] is not None else None
            enrichments["missing_required_aspects"] = aspect_result["missing_required_aspects"]

        if self.simplemma is not None and alpha_tokens:
            lemma_tokens = [
                self.simplemma.lemmatize(token.lower(), lang=self.config.get("expected_language", "bg"))
                for token in alpha_tokens
            ]
            lemma_unique_ratio = _safe_ratio(len(set(lemma_tokens)), len(lemma_tokens))
            enrichments["lemma_unique_ratio"] = round(lemma_unique_ratio, 4)
            known_word_ratio = self.simplemma.in_target_language(
                clean_text,
                lang=self.config.get("expected_language", "bg"),
            )
            enrichments["simplemma_known_word_ratio"] = round(float(known_word_ratio), 4)
            if lemma_unique_ratio < float(self.config.get("warn_low_lemma_unique_ratio", 0.4)):
                issues.append("low_lemma_unique_ratio")
                score -= 5
            if char_count >= recommended_min_chars and known_word_ratio < 0.45:
                issues.append("low_known_word_ratio")
                score -= 8

        if self.wordfreq is not None and alpha_tokens:
            zipf_values = [
                self.wordfreq.zipf_frequency(token.lower(), self.config.get("expected_language", "bg"))
                for token in alpha_tokens
                if len(token) >= 3
            ]
            if zipf_values:
                enrichments["avg_zipf_frequency"] = round(mean(zipf_values), 4)
                enrichments["known_zipf_token_ratio"] = round(
                    _safe_ratio(sum(1 for value in zipf_values if value > 0.0), len(zipf_values)),
                    4,
                )

        if self.lingua is not None and char_count >= 20:
            expected_language = str(self.config.get("expected_language", "bg")).lower()
            candidate_codes = tuple(dict.fromkeys([expected_language, "en"]))
            detector = _lingua_detector(candidate_codes)
            if detector is not None:
                detected = detector.detect_language_of(clean_text)
                enrichments["lingua_detected_language"] = getattr(detected, "iso_code_639_1", None).name.lower() if detected else None
                if detected is not None:
                    detected_code = enrichments["lingua_detected_language"]
                    if detected_code and detected_code != expected_language:
                        issues.append("language_mismatch")
                        score -= 8

        score = max(0, min(100, score))
        if score >= 85:
            band = "strong"
        elif score >= 70:
            band = "good"
        elif score >= 50:
            band = "fair"
        else:
            band = "poor"

        normalized_key = _normalize_whitespace(clean_text.lower())

        return {
            "score": score,
            "band": band,
            "issues": sorted(set(issues)),
            "normalized_key": normalized_key,
            "aspect_coverage": aspect_result,
            "metrics": {
                "char_count": char_count,
                "token_count": token_count,
                "sentence_count": sentence_count,
                "measurement_hits": measurement_hits,
                "all_caps_ratio": round(all_caps_ratio, 4),
                "unique_token_ratio": round(unique_token_ratio, 4),
                "title_overlap_ratio": round(title_overlap_ratio, 4),
                "has_html": has_html,
                "has_url": has_url,
            },
            "enrichments": enrichments,
            "clean_excerpt": clean_text[:220],
        }
