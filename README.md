# AG1 Attribution Analysis

A dbt pipeline on BigQuery that unifies Shopify, Meta Ads, and Klaviyo data to surface the gap between what marketing platforms report and what actually happened in the store.

## The Problem

DTC brands routinely over-report revenue because Meta and Klaviyo use overlapping attribution windows. Every platform takes credit for the same order.

| Platform | Reported Revenue | Actual (Shopify) | Overclaim |
|---|---|---|---|
| Meta Ads | $246,225 | $131,450 | +87.5% |
| Klaviyo | $120,046 | $90,310 | +33.0% |
| **Combined** | **$366,271** | **$221,760** | **+$145k** |

Meta's true ROAS is **0.99** — the brand thinks paid social is profitable. It's barely breaking even. Meanwhile email drives $90k in revenue with zero ad spend.

## Project Structure

```
models/
├── staging/
│   ├── stg_shopify_orders.sql       -- cleans raw orders, casts types, filters nulls
│   ├── stg_meta_ads.sql             -- normalizes campaign/adset/ad grain, renames columns
│   └── stg_klaviyo_campaigns.sql    -- unifies campaigns and flows
├── intermediate/
│   └── int_orders_with_channel.sql  -- deduplicates orders, assigns clean channel labels
└── marts/
    ├── mart_revenue_by_channel.sql  -- true vs. reported revenue by channel
    ├── mart_ad_performance.sql      -- Meta ROAS, CPC, CPM, true conversion rate
    └── mart_email_performance.sql   -- Klaviyo sends, clicks, revenue per recipient
```

## Key Findings

- **Meta overclaims by 87.5%** — 7-day click / 1-day view attribution window double-counts orders that were already in the purchase funnel
- **Klaviyo overclaims by 33%** — 5-day attribution window captures organic repurchases
- **True Meta ROAS: 0.99** vs. reported 1.86 — paid social is not profitable at current spend levels
- **Email is the highest-ROI channel** — $90k revenue, $0 ad spend
- **116 duplicate order IDs** caught in raw data and deduplicated before any revenue aggregation

## Data Quality

Raw data was built to reflect real-world conditions: null emails, duplicate order IDs, future-dated records, and negative order amounts. dbt tests catch these at the source before they contaminate downstream models.

## Tech Stack

- **dbt Core** — transformation, testing, lineage
- **BigQuery** — warehouse
- **Python** — synthetic data generation (Shopify, Meta, Klaviyo CSVs)
- **SQL** — all business logic

## How to Run

```bash
pip install dbt-bigquery

dbt run
dbt test
```

Requires a `profiles.yml` configured for BigQuery with a service account key. See [dbt BigQuery setup docs](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup).
