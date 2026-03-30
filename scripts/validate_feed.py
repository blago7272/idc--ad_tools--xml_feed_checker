#!/usr/bin/env python3
"""Stream and validate Google/Meta XML catalog feeds."""

from __future__ import annotations

import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from description_quality import DescriptionAnalyzer

MONEY_RE = re.compile(r"^\s*(\d+(?:\.\d{1,2})?)\s+([A-Z]{3})\s*$")
SAMPLE_LIMIT = 5


def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def is_valid_url(value: str) -> bool:
    if not value:
        return False
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def has_double_slash_path(value: str) -> bool:
    parsed = urlparse(value)
    return bool(parsed.path.startswith("//") or "//" in parsed.path)


def parse_money(value: str) -> Tuple[Optional[float], Optional[str]]:
    match = MONEY_RE.match(value or "")
    if not match:
        return None, None
    return float(match.group(1)), match.group(2)


def percentile(values: List[int], ratio: float) -> Optional[int]:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, round((len(ordered) - 1) * ratio)))
    return ordered[index]


def stats_summary(values: List[int]) -> Dict[str, Optional[float]]:
    if not values:
        return {"min": None, "p50": None, "p90": None, "max": None}
    return {
        "min": min(values),
        "p50": median(values),
        "p90": percentile(values, 0.9),
        "max": max(values),
    }


def add_sample(store: Dict[str, List[Dict[str, object]]], key: str, sample: Dict[str, object], limit: int) -> None:
    bucket = store[key]
    if len(bucket) < limit:
        bucket.append(sample)


def feed_slug(path: Path) -> str:
    parent = path.parent.name or "feed"
    stem = path.stem or "products"
    raw = f"{parent}__{stem}"
    return re.sub(r"[^A-Za-z0-9._-]+", "-", raw)


def parse_feed(
    path: Path,
    rules: Dict[str, object],
    platforms: Iterable[str],
    analyzer: DescriptionAnalyzer,
) -> Dict[str, object]:
    required_fields = list(rules["required_fields"])
    expected_fields = list(rules["expected_fields"])
    url_fields = list(rules["url_fields"])
    exact_values = dict(rules["exact_values"])
    expected_currency = str(rules["expected_currency"])
    description_rules = dict(rules["description_quality"])
    platform_rules = dict(rules["platforms"])
    sample_limit = int(description_rules.get("sample_size_per_issue", SAMPLE_LIMIT))

    errors = Counter()
    warnings = Counter()
    samples: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    field_presence = Counter()
    field_sets = Counter()
    ids = Counter()
    value_counts = defaultdict(Counter)
    description_duplicates = Counter()

    title_lengths: List[int] = []
    description_lengths: List[int] = []
    description_scores: List[int] = []
    description_issue_counts = Counter()
    aspect_group_counts = Counter()
    aspect_coverage_bands = Counter()
    aspect_gap_counts = Counter()
    required_aspect_coverages: List[float] = []
    optional_aspect_coverages: List[float] = []

    item_count = 0

    for _, elem in ET.iterparse(path, events=("end",)):
        if local_name(elem.tag) not in {"entry", "item"}:
            continue

        item_count += 1
        values: Dict[str, str] = {}
        for child in list(elem):
            tag = local_name(child.tag)
            text = (child.text or "").strip()
            values[tag] = text
            field_presence[tag] += 1
        field_sets[tuple(sorted(values.keys()))] += 1

        item_id = values.get("id", f"row-{item_count}")
        if item_id:
            ids[item_id] += 1

        sample_context = {
            "id": item_id,
            "title": values.get("title"),
            "link": values.get("link"),
        }

        for field in required_fields:
            if field not in values:
                errors[f"missing_required:{field}"] += 1
                add_sample(samples, f"missing_required:{field}", sample_context, sample_limit)
            elif not values[field]:
                errors[f"empty_required:{field}"] += 1
                add_sample(samples, f"empty_required:{field}", sample_context, sample_limit)

        for field in expected_fields:
            if field not in values:
                warnings[f"missing_expected:{field}"] += 1
                add_sample(samples, f"missing_expected:{field}", sample_context, sample_limit)
            elif not values[field]:
                warnings[f"empty_expected:{field}"] += 1
                add_sample(samples, f"empty_expected:{field}", sample_context, sample_limit)

        for field in ("condition", "availability", "brand", "google_product_category", "product_type", "size"):
            value = values.get(field)
            if value:
                value_counts[field][value] += 1

        title = values.get("title", "")
        description = values.get("description", "")
        if title:
            title_lengths.append(len(title))
        if description:
            description_lengths.append(len(description))

        for field in url_fields:
            value = values.get(field, "")
            if not value:
                continue
            if not is_valid_url(value):
                errors[f"invalid_url:{field}"] += 1
                add_sample(
                    samples,
                    f"invalid_url:{field}",
                    {**sample_context, "value": value},
                    sample_limit,
                )
            elif has_double_slash_path(value):
                warnings[f"double_slash_path:{field}"] += 1
                add_sample(
                    samples,
                    f"double_slash_path:{field}",
                    {**sample_context, "value": value},
                    sample_limit,
                )

        for field, expected_value in exact_values.items():
            value = values.get(field)
            if value and value != expected_value:
                errors[f"unexpected_value:{field}"] += 1
                add_sample(
                    samples,
                    f"unexpected_value:{field}",
                    {**sample_context, "value": value, "expected": expected_value},
                    sample_limit,
                )

        price_amount, price_currency = parse_money(values.get("price", ""))
        sale_amount, sale_currency = parse_money(values.get("sale_price", ""))

        if values.get("price") and price_amount is None:
            errors["invalid_money:price"] += 1
            add_sample(samples, "invalid_money:price", {**sample_context, "value": values.get("price")}, sample_limit)
        if values.get("sale_price") and sale_amount is None:
            errors["invalid_money:sale_price"] += 1
            add_sample(
                samples,
                "invalid_money:sale_price",
                {**sample_context, "value": values.get("sale_price")},
                sample_limit,
            )

        if price_currency and price_currency != expected_currency:
            errors["unexpected_currency:price"] += 1
            add_sample(
                samples,
                "unexpected_currency:price",
                {**sample_context, "value": values.get("price")},
                sample_limit,
            )
        if sale_currency and sale_currency != expected_currency:
            errors["unexpected_currency:sale_price"] += 1
            add_sample(
                samples,
                "unexpected_currency:sale_price",
                {**sample_context, "value": values.get("sale_price")},
                sample_limit,
            )

        if price_amount is not None and sale_amount is not None:
            if any(platform_rules[name]["sale_price_must_be_lower_than_price"] for name in platforms) and sale_amount >= price_amount:
                errors["sale_price_not_lower_than_price"] += 1
                add_sample(
                    samples,
                    "sale_price_not_lower_than_price",
                    {
                        **sample_context,
                        "price": values.get("price"),
                        "sale_price": values.get("sale_price"),
                    },
                    sample_limit,
                )
            if price_amount == 0 and sale_amount > 0:
                warnings["zero_price_with_positive_sale_price"] += 1
                add_sample(
                    samples,
                    "zero_price_with_positive_sale_price",
                    {
                        **sample_context,
                        "price": values.get("price"),
                        "sale_price": values.get("sale_price"),
                    },
                    sample_limit,
                )

        description_result = analyzer.analyze(
            description,
            title=title,
            product_type=values.get("product_type", ""),
        )
        description_scores.append(int(description_result["score"]))
        description_duplicates[description_result["normalized_key"]] += 1

        aspect_result = description_result.get("aspect_coverage", {})
        if aspect_result.get("group_name"):
            aspect_group_counts[aspect_result["group_name"]] += 1
        if aspect_result.get("coverage_band"):
            aspect_coverage_bands[aspect_result["coverage_band"]] += 1
        if aspect_result.get("required_ratio") is not None:
            required_aspect_coverages.append(float(aspect_result["required_ratio"]))
        if aspect_result.get("optional_ratio") is not None:
            optional_aspect_coverages.append(float(aspect_result["optional_ratio"]))
        for aspect_id in aspect_result.get("missing_required_aspects", []):
            aspect_gap_counts[f"{aspect_result.get('group_name', 'unknown')}:{aspect_id}"] += 1

        for issue in description_result["issues"]:
            description_issue_counts[issue] += 1
            if issue != "empty_description":
                warnings[f"description:{issue}"] += 1
                add_sample(
                    samples,
                    f"description:{issue}",
                    {
                        **sample_context,
                        "score": description_result["score"],
                        "excerpt": description_result["clean_excerpt"],
                    },
                    sample_limit,
                )

        for platform in platforms:
            max_chars = int(platform_rules[platform]["description_max_chars"])
            if description and len(description) > max_chars:
                errors[f"description_over_limit:{platform}"] += 1
                add_sample(
                    samples,
                    f"description_over_limit:{platform}",
                    {
                        **sample_context,
                        "length": len(description),
                        "limit": max_chars,
                    },
                    sample_limit,
                )

        meta_title_max = platform_rules.get("meta_ads", {}).get("title_recommended_max_chars")
        if "meta_ads" in platforms and meta_title_max and title and len(title) > int(meta_title_max):
            warnings["title_over_meta_recommendation"] += 1
            add_sample(
                samples,
                "title_over_meta_recommendation",
                {**sample_context, "length": len(title)},
                sample_limit,
            )

        elem.clear()

    duplicate_id_groups = {feed_id: count for feed_id, count in ids.items() if count > 1}
    if duplicate_id_groups:
        errors["duplicate_ids"] += sum(count - 1 for count in duplicate_id_groups.values())
        for duplicate_id, count in list(duplicate_id_groups.items())[:sample_limit]:
            add_sample(samples, "duplicate_ids", {"id": duplicate_id, "count": count}, sample_limit)

    duplicate_descriptions = [
        {"count": count, "text": key[:240]}
        for key, count in description_duplicates.most_common()
        if key and count > 1
    ][:sample_limit]

    total_errors = sum(errors.values())
    total_warnings = sum(warnings.values())
    status = "fail" if total_errors else "warn" if total_warnings else "pass"

    return {
        "feed_path": str(path),
        "feed_slug": feed_slug(path),
        "status": status,
        "platforms": list(platforms),
        "item_count": item_count,
        "backend_status": analyzer.backend_status(),
        "errors": dict(errors),
        "warnings": dict(warnings),
        "samples": dict(samples),
        "field_presence": dict(field_presence),
        "distinct_field_sets": [
            {"fields": list(field_set), "count": count}
            for field_set, count in field_sets.most_common(5)
        ],
        "value_counts": {
            field: dict(counter.most_common(10))
            for field, counter in value_counts.items()
        },
        "description": {
            "length_stats": stats_summary(description_lengths),
            "score_stats": stats_summary(description_scores),
            "issue_counts": dict(description_issue_counts),
            "aspect_summary": {
                "group_counts": dict(aspect_group_counts),
                "coverage_bands": dict(aspect_coverage_bands),
                "required_coverage_stats": stats_summary(required_aspect_coverages),
                "optional_coverage_stats": stats_summary(optional_aspect_coverages),
                "gap_counts": dict(aspect_gap_counts.most_common(20)),
            },
            "duplicate_examples": duplicate_descriptions,
        },
        "title": {
            "length_stats": stats_summary(title_lengths),
        },
    }


def render_markdown(report: Dict[str, object]) -> str:
    lines = [
        f"# Feed Report: {Path(report['feed_path']).name}",
        "",
        f"- Feed: `{report['feed_path']}`",
        f"- Status: `{report['status']}`",
        f"- Platforms: `{', '.join(report['platforms'])}`",
        f"- Items: `{report['item_count']}`",
        "",
        "## Backends",
        "",
    ]

    for backend, enabled in report["backend_status"].items():
        lines.append(f"- `{backend}`: `{'available' if enabled else 'not installed'}`")

    lines.extend(["", "## Errors", ""])
    if report["errors"]:
        for key, value in sorted(report["errors"].items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"- `{key}`: `{value}`")
    else:
        lines.append("- None")

    lines.extend(["", "## Warnings", ""])
    if report["warnings"]:
        for key, value in sorted(report["warnings"].items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"- `{key}`: `{value}`")
    else:
        lines.append("- None")

    lines.extend(["", "## Description Summary", ""])
    desc = report["description"]
    lines.append(f"- Length stats: `{desc['length_stats']}`")
    lines.append(f"- Score stats: `{desc['score_stats']}`")
    if desc.get("aspect_summary"):
        lines.append(f"- Aspect coverage bands: `{desc['aspect_summary']['coverage_bands']}`")
        lines.append(f"- Required aspect coverage stats: `{desc['aspect_summary']['required_coverage_stats']}`")
        lines.append(f"- Optional aspect coverage stats: `{desc['aspect_summary']['optional_coverage_stats']}`")
    if desc["issue_counts"]:
        lines.append("- Issue counts:")
        for key, value in sorted(desc["issue_counts"].items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"  - `{key}`: `{value}`")
    if desc.get("aspect_summary", {}).get("gap_counts"):
        lines.append("- Top aspect gaps:")
        for key, value in desc["aspect_summary"]["gap_counts"].items():
            lines.append(f"  - `{key}`: `{value}`")
    if desc["duplicate_examples"]:
        lines.append("- Duplicate description examples:")
        for item in desc["duplicate_examples"]:
            lines.append(f"  - `{item['count']}` uses: `{item['text']}`")

    lines.extend(["", "## Top Values", ""])
    for field, values in report["value_counts"].items():
        lines.append(f"- `{field}`: `{values}`")

    lines.extend(["", "## Field Sets", ""])
    for item in report["distinct_field_sets"]:
        lines.append(f"- `{item['count']}` items: `{', '.join(item['fields'])}`")

    if report["samples"]:
        lines.extend(["", "## Samples", ""])
        for key, examples in sorted(report["samples"].items()):
            lines.append(f"### `{key}`")
            for example in examples:
                lines.append(f"- `{json.dumps(example, ensure_ascii=False)}`")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def write_report(report: Dict[str, object], output_dir: Path) -> Tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = report["feed_slug"]
    json_path = output_dir / f"{stem}.report.json"
    md_path = output_dir / f"{stem}.report.md"

    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, md_path


def build_parser() -> argparse.ArgumentParser:
    project_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("feeds", nargs="+", help="Paths to XML feed files.")
    parser.add_argument(
        "--config",
        default=str(project_root / "config" / "feed_rules.json"),
        help="Path to the JSON rule file.",
    )
    parser.add_argument(
        "--platforms",
        nargs="+",
        default=["google_ads", "meta_ads"],
        choices=["google_ads", "meta_ads"],
        help="Platforms to validate against.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(project_root / "out"),
        help="Directory for JSON and Markdown reports.",
    )
    parser.add_argument(
        "--aspect-config",
        default=str(project_root / "config" / "semantic_aspects.json"),
        help="Path to the aspect rule JSON file.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config_path = Path(args.config).expanduser().resolve()
    output_dir = Path(args.out_dir).expanduser().resolve()
    aspect_config_path = Path(args.aspect_config).expanduser().resolve()

    rules = json.loads(config_path.read_text(encoding="utf-8"))
    aspect_groups = {}
    if aspect_config_path.exists():
        aspect_groups = json.loads(aspect_config_path.read_text(encoding="utf-8")).get("groups", {})
    analyzer = DescriptionAnalyzer(rules["description_quality"], aspect_groups=aspect_groups)

    for platform in args.platforms:
        if platform not in rules["platforms"]:
            parser.error(f"Unknown platform: {platform}")

    reports = []
    for feed in args.feeds:
        feed_path = Path(feed).expanduser().resolve()
        if not feed_path.exists():
            parser.error(f"Feed not found: {feed_path}")
        report = parse_feed(feed_path, rules, args.platforms, analyzer)
        json_path, md_path = write_report(report, output_dir)
        reports.append((report, json_path, md_path))

    for report, json_path, md_path in reports:
        print(
            json.dumps(
                {
                    "feed": report["feed_path"],
                    "status": report["status"],
                    "items": report["item_count"],
                    "errors": sum(report["errors"].values()),
                    "warnings": sum(report["warnings"].values()),
                    "json_report": str(json_path),
                    "markdown_report": str(md_path),
                },
                ensure_ascii=False,
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
