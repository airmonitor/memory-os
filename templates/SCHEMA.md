# Wiki Schema Template

This document defines the structure for wiki pages in the Memory OS knowledge base.
Each page under `wiki/{concepts,entities,comparisons}/` should follow this structure.

## Frontmatter

```yaml
---
title: "Page Title"
type: concept          # concept | entity | comparison
tags: [tag1, tag2]
created: YYYY-MM-DD
updated: YYYY-MM-DD
source: raw/filename.md # link to source document
status: seedling       # seedling | growing | evergreen
aliases: [alt-name-1, alt-name-2]
---
```

**Field descriptions:**
- `type` — one of: `concept` (abstract pattern/idea), `entity` (concrete tool/project/person), `comparison` (side-by-side analysis)
- `status` — maturity indicator: `seedling` (new), `growing` (being refined), `evergreen` (stable reference)
- `source` — relative path to the raw source document that generated this page
- `aliases` — alternative names for cross-linking and search

## Body Structure

### For `concept` pages

```markdown
# Concept Name

## Summary

One-paragraph high-level overview of the concept.

## Description

Detailed explanation. Include:
- What problem this concept solves
- How it works at a high level
- Key principles or rules

## Examples

### Example 1: Descriptive title
```
code or configuration block
```
Brief explanation of what the example demonstrates.

### Example 2: Another example

## Related

- [[Related Concept 1]] — relationship description
- [[Related Concept 2]] — relationship description
```

### For `entity` pages

```markdown
# Entity Name

## Summary

What this thing is — one paragraph.

## Purpose

Why this entity exists in the system.

## Configuration

```yaml
# Example configuration block
key: value
option: setting
```

## Dependencies

- Dependency 1: what it provides
- Dependency 2: what it provides

## Usage Notes

Practical considerations, edge cases, known issues.

## Related

- [[Related Concept]] — relationship
```

### For `comparison` pages

```markdown
# Comparison: A vs B

## Summary

One-paragraph overview of what is being compared.

## Comparison Table

| Aspect | Option A | Option B |
|--------|----------|----------|
| Strengths | ... | ... |
| Weaknesses | ... | ... |
| Best for | ... | ... |

## Decision Factors

Considerations that favour one option over the other.

## Recommendation

Final recommendation with reasoning.

## Related

- [[Option A detail page]]
- [[Option B detail page]]
```
