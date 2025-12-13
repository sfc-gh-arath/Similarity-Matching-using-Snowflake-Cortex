# Similarity-Matching-using-Snowflake-Cortex
Similarity Matching using Snowflake Cortex (vector embedding)

This repos contains code used in 
https://medium.com/@aswinee.rath/similarity-matching-using-snowflake-cortex-vector-embedding-3d46e8de06a4


and other supporting code


# Address Similarity Matching Notebook

A Snowflake Notebook demonstrating address matching and customer similarity analysis using **Cortex Search**, **Vector Embeddings**, and **LLM-powered scoring**.

## Overview

This notebook showcases multiple approaches to similarity matching in Snowflake:

1. **Address Matching** — Find existing customer records that match new incoming addresses
2. **Match Quality Verification** — Validate matches using vector cosine similarity
3. **LLM-Based Scoring** — Use Cortex Complete to generate human-readable match scores
4. **Customer Similarity** — Find similar customers based on order history for upselling opportunities

## Prerequisites

- Snowflake account with Cortex features enabled
- `ACCOUNTADMIN` role (or equivalent privileges)
- Pre-configured database and tables (see [Setup](#setup))
- Cortex Search Service: `customer_master_data_search_svc`

## Setup

### Required Database Objects

```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE customer_support;
USE SCHEMA support;
USE WAREHOUSE COMPUTE_WH;
```

### Required Tables

| Table | Description |
|-------|-------------|
| `customer_support.support.customer_master_data` | Master customer records (~1M rows) |
| `customer_support.support.new_customer_data` | New records to match against master |
| `DEMO_DB.CRM.orders` | Customer order data (for similarity analysis) |
| `DEMO_DB.CRM.orders_vector` | Vectorized order data |

### Cortex Search Service

```sql
CREATE OR REPLACE CORTEX SEARCH SERVICE customer_master_data_search_svc
  ON full_details
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 day'
  AS (
    SELECT full_details
    FROM customer_master_data
  );
```

## Notebook Walkthrough

### Part 1: Address Matching with Cortex Search

**Cells 1-5** — Initialize session and test Cortex Search:

```python
from snowflake.core import Root

root = Root(session)
svc = (root
  .databases["customer_support"]
  .schemas["support"]
  .cortex_search_services["customer_master_data_search_svc"]
)

# Test single query
resp = svc.search(
  query="Patton 887 Aaron Center WY 15339",
  columns=["full_details"],
  limit=1
).to_json()
```

**Cell 7** — Batch match all new addresses:

```python
new_data = session.table("customer_support.support.new_customer_data").select("FULL_DETAILS").to_pandas()
df = pd.DataFrame(columns=['New Address', 'Matching Existing address'])

for index, row in new_data.iterrows():
    resp = svc.search(
        query=row["FULL_DETAILS"],
        columns=["full_details"],
        limit=1
    ).to_json()
    j = json.loads(resp)
    df.loc[index] = [row["FULL_DETAILS"], j["results"][0]["full_details"]]
```

### Part 2: Match Quality Verification

**Cell 12** — Calculate cosine similarity between matched pairs:

```sql
WITH compare_data_vector AS (
  SELECT 
    *,
    snowflake.cortex.embed_text('e5-base-v2', "New Address") AS new_addr_vector, 
    snowflake.cortex.embed_text('e5-base-v2', "Matching Existing address") AS matching_addr_vector
  FROM "compared_data"
)
SELECT
   "New Address",
   "Matching Existing address",
   VECTOR_COSINE_SIMILARITY(new_addr_vector, matching_addr_vector) AS score
FROM compare_data_vector;
```

**Score Interpretation:**
- `1.0` = Identical match
- `0.8+` = Strong match
- `0.5-0.8` = Partial match
- `< 0.5` = Weak match

### Part 3: LLM-Powered Match Scoring

**Cell 13** — Use Mistral-7B to generate match scores:

```sql
SELECT 
  snowflake.cortex.complete('mistral-7b', 
    'How close these two given addresses. 
    <address_1>' || "New Address" || '</address_1>
    <address_2>' || "Matching Existing address" || '</address_2>
    Provide a matching score of 1 to 100, where 100 being identical and 1 no matches at all. 
    Just provide the score, no other verbiage'
  ) AS MATCH_DETAILS,
  "New Address",
  "Matching Existing address"
FROM "compared_data";
```

### Part 4: Customer Similarity for Upselling

**Cells 16-17** — Find similar customers based on order history:

```sql
-- Find best matching customer and identify upsell opportunities
SELECT
   v.cust_id,
   v.agg AS products_ordered,
   m.cust_id AS best_matching_customer,
   m.agg AS matching_products,
   array_except(STRTOK_TO_ARRAY(m.agg,','), STRTOK_TO_ARRAY(v.agg,',')) AS UPSELL_OPP,
   VECTOR_COSINE_SIMILARITY(v.agg_vector, m.agg_vector) AS match_score
FROM 
    DEMO_DB.CRM.orders_vector v 
    INNER JOIN DEMO_DB.CRM.orders_vector m ON v.cust_id != m.cust_id
QUALIFY RANK() OVER(PARTITION BY v.cust_id ORDER BY match_score DESC) = 1
ORDER BY v.cust_id;
```

This identifies:
- **Best matching customer** — Customer with most similar order history
- **Match score** — How similar the ordering patterns are
- **Upsell opportunities** — Products the matching customer bought that this customer hasn't

## Key Snowflake Cortex Functions Used

| Function | Purpose |
|----------|---------|
| `cortex_search_services[...].search()` | Semantic search over text data |
| `snowflake.cortex.embed_text()` | Convert text to vector embeddings |
| `VECTOR_COSINE_SIMILARITY()` | Calculate similarity between vectors |
| `snowflake.cortex.complete()` | LLM text generation |

## Use Cases

1. **Customer Deduplication** — Identify duplicate customer records with variations
2. **Data Quality** — Validate and score address matching accuracy
3. **Customer Segmentation** — Group similar customers by behavior
4. **Upselling** — Recommend products based on similar customer purchases

## Output

The notebook produces:
- A DataFrame showing new addresses matched to existing records
- Cosine similarity scores for each match
- LLM-generated match scores (1-100 scale)
- Customer similarity analysis with upsell recommendations

## References

- [Snowflake Cortex Search](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)
- [Cortex LLM Functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions)
- [Vector Similarity Functions](https://docs.snowflake.com/en/sql-reference/functions/vector_cosine_similarity)

