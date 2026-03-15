# GBB Type Split Rules

These rules define how each GBB Type in the SAS sheet drives the split behaviour.
The source of truth in code is `GBB_TYPE_RULES` in `bop_splitter/salience.py`.

| GBB Type | Default Split Level | Action | Notes |
|----------|-------------------|--------|-------|
| Brand Building Activities | Form | split | Use all SKUs |
| Promotions - Go To Market | Form | exceptions | Use all SKUs or ask which SKUs — routes user to Exception list |
| New Channels | Form | exceptions | Ask which SKUs — routes user to Exception list |
| Initiatives | Form | ignore | Exclude from split; add to exception list; prompt user to provide manual inputs |
| Pricing Strategy | Brand | split | Split across the brand |
| Market Trend | Sub Brand | split | Split at Sub Brand level |
| Customer Inventory Strategy | Form (user-defined) | split | Use all SKUs; split level chosen by user |

## Action semantics

- **split** — normal split using salience weights at the specified hierarchy level
- **exceptions** — BB is split but user must confirm/select which SKUs receive allocation via the Exception list (Step 5)
- **ignore** — BB is excluded from the split entirely; all SKUs are added to the exclusion list with a note; user must provide manual inputs outside the tool

## User-defined split level

When `user_defined = True` (Customer Inventory Strategy), the Default Split Level shown above is only a suggestion.
The user may change the Split Level for that BB in the Step 3 editor and the chosen value will be used in the split.

## Adding or changing rules

Edit `GBB_TYPE_RULES` in `bop_splitter/salience.py`. Each entry must have:

```python
"GBB Type label": {
    "split_level": "<one of the keys in SPLIT_KEYS>",
    "action": "split" | "exceptions" | "ignore",
    "user_defined": True | False,
    "description": "<human-readable note>",
}
```

Then update this file to keep the table in sync.
