---
name: xml-feed-quality-checker
description: Validate XML product and catalog feeds for Google Ads and Meta Ads by checking required fields, URLs, money formats, exact-value rules, variant completeness, duplicate IDs, and description quality with category-aware aspect coverage. Use when auditing XML feeds, debugging feed generation, comparing feed quality over time, or producing remediation reports for any XML-producing source system.
---

# XML Feed Quality Checker

## Overview
Use this skill to validate one or more XML catalog feeds from any source system. The bundled validator produces deterministic JSON and Markdown reports with blocking errors, quality warnings, category-level aspect gaps, and remediation-ready samples.

## Workflow
1. Collect the feed path or paths to validate.
2. Choose a bundled profile or disable profiles and supply custom config files.
3. Run the validator script.
4. Review the Markdown report first for a readable summary, then the JSON report for automation or deeper analysis.
5. Fix feed-generation or source-data issues at the source layer instead of patching the exported XML manually.

## Commands
- Default validation with the bundled Google + Meta rules:
```bash
python3 scripts/validate_feed.py /path/to/feed.xml
```

- List bundled profiles:
```bash
python3 scripts/validate_feed.py --list-profiles
```

- Validate with a bundled profile:
```bash
python3 scripts/validate_feed.py \
  /path/to/feed.xml \
  --profile google-only
```

- Validate multiple feeds in one run:
```bash
python3 scripts/validate_feed.py /path/to/feed-a.xml /path/to/feed-b.xml
```

- Disable profiles and use full custom config files:
```bash
python3 scripts/validate_feed.py \
  /path/to/feed.xml \
  --profile none \
  --config /path/to/feed_rules.json \
  --aspect-config /path/to/semantic_aspects.json \
  --out-dir /path/to/out
```

- Keep a bundled profile and merge partial overrides on top:
```bash
python3 scripts/validate_feed.py \
  /path/to/feed.xml \
  --profile google-meta-default \
  --config-override /path/to/rules.override.json \
  --aspect-override /path/to/aspects.override.json
```

## What The Validator Checks
- Required and expected fields.
- Empty required values.
- URL validity and double-slash path defects.
- Money format and expected currency.
- Exact-value constraints such as `condition = new` and `availability = in stock`.
- Pricing logic such as `sale_price < price`.
- Duplicate IDs.
- Description quality:
  - length and thin copy
  - duplicate descriptions
  - missing numeric detail
  - title overlap
  - category-specific aspect coverage

## Default Files
- Validator: `scripts/validate_feed.py`
- Description analyzer: `scripts/description_quality.py`
- Base rules: `config/feed_rules.json`
- Category aspect rules: `config/semantic_aspects.json`
- Bundled profiles: `config/profiles/*.json`

## Interpreting Results
- Treat these as blocking:
  - invalid price and sale price logic
  - invalid URLs
  - missing required fields
  - empty required descriptions
- Treat these as quality warnings:
  - missing dimensions/specs
  - low aspect coverage
  - thin descriptions
  - duplicate low-information descriptions

## Customization Guidance
- Use a bundled profile when the default Google/Meta combinations are enough.
- Use `--config` and `--aspect-config` when you want to replace the base config files entirely.
- Use `--config-override` and `--aspect-override` when you only need a small delta on top of a bundled profile or base config.
- If the feed uses different currencies, required fields, or exact-value rules, adjust the config files rather than editing the validator logic first.
- If the feed uses different product families, adapt the category aspect rules in the aspect config.
- Keep the core skill source-agnostic. Platform-specific mapping belongs in the input config, not in the main workflow.

## Reporting Guidance
- Start with the highest-count blocking issues.
- Summarize shared defects first, then feed-specific content gaps.
- Use the category aspect gaps to drive description templates or source-attribute backfills.
- Recommend fixes at the source-data or feed-generation layer wherever possible.
