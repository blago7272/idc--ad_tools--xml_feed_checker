# Usage Guide

## Purpose

Use this skill to validate XML product feeds for catalog quality, with default support for Google Ads and Meta Ads rules.

The skill is source-agnostic:

- ecommerce platform exports
- PIM exports
- ERP exports
- custom XML generators

The only requirement is an XML feed file that follows a product-entry structure the validator can inspect.

## Quick Start

Validate one feed with the bundled default profile:

```bash
python3 scripts/validate_feed.py /path/to/feed.xml
```

Validate multiple feeds in one run:

```bash
python3 scripts/validate_feed.py /path/to/feed-a.xml /path/to/feed-b.xml
```

Output:

- one Markdown report per feed
- one JSON report per feed

Default output directory:

- `./out`

## Bundled Profiles

List bundled profiles:

```bash
python3 scripts/validate_feed.py --list-profiles
```

Current bundled profiles:

- `google-meta-default`
- `google-only`
- `meta-only`

Run with a bundled profile:

```bash
python3 scripts/validate_feed.py \
  /path/to/feed.xml \
  --profile google-only
```

When to use profiles:

- use `google-meta-default` for the standard combined audit
- use `google-only` when you only care about Google Ads checks
- use `meta-only` when you only care about Meta Ads checks

## Full Custom Config Mode

Use this mode when you want to replace the bundled rule files entirely.

```bash
python3 scripts/validate_feed.py \
  /path/to/feed.xml \
  --profile none \
  --config /path/to/feed_rules.json \
  --aspect-config /path/to/semantic_aspects.json
```

Use this when:

- you have a different required field set
- you need a different currency
- you want a different category aspect model

## Partial Override Mode

Use this mode when the bundled profile is mostly correct and you only need a small delta.

```bash
python3 scripts/validate_feed.py \
  /path/to/feed.xml \
  --profile google-meta-default \
  --config-override /path/to/rules.override.json \
  --aspect-override /path/to/aspects.override.json
```

Use this when:

- you want to override a small subset of rules
- you want to keep the bundled base config
- you want a cleaner maintenance model than duplicating the entire config

## Typical Output

The reports include:

- overall status
- active platforms
- active profile
- rule file paths used for the run
- errors
- warnings
- description quality summary
- category aspect gaps
- representative samples

## How To Read Findings

Treat these as blocking:

- missing required fields
- empty required fields
- invalid URLs
- invalid money values
- invalid price / sale price logic

Treat these as quality warnings:

- thin descriptions
- missing dimensions/specifications
- low aspect coverage
- repeated low-information descriptions
- title over Meta recommendation

## Recommended Workflow

1. Run the validator on the current feed.
2. Review the Markdown report first.
3. Fix the highest-count blocking issues in the source system or feed generator.
4. Re-run the validator.
5. Use the aspect-gap output to improve description templates and source attributes.

## Common Patterns

Validate one feed and write reports to a specific directory:

```bash
python3 scripts/validate_feed.py \
  /path/to/feed.xml \
  --out-dir /path/to/out
```

Validate with an explicit platform override:

```bash
python3 scripts/validate_feed.py \
  /path/to/feed.xml \
  --platforms google_ads
```

Validate with a profile and a small rules override:

```bash
python3 scripts/validate_feed.py \
  /path/to/feed.xml \
  --profile meta-only \
  --config-override /path/to/rules.override.json
```

## Notes

- Prefer fixing issues in the source-data or feed-generation layer rather than patching exported XML manually.
- Keep custom rule deltas small when possible.
- Use full config replacement only when the bundled model is not a good fit.
