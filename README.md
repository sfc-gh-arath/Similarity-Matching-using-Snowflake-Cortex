# Similarity-Matching-using-Snowflake-Cortex
Similarity Matching using Snowflake Cortex (vector embedding)

This repos contains code used in 
https://medium.com/@aswinee.rath/similarity-matching-using-snowflake-cortex-vector-embedding-3d46e8de06a4
and other supporting code

A Snowflake-based solution for entity matching and deduplication using **Cortex Search Service** and **vector embeddings**. Match new customer records against existing master data using semantic similarity.

## Project Structure

| File | Description |
|------|-------------|
| `SimilarityMatching.sql` | SQL script to build database, tables, and vector embeddings |
| `AddressSimilarityMatching.ipynb` | Snowflake Notebook for interactive similarity matching |

---

## Part 1: Build the Database and Vectors

Run `SimilarityMatching.sql` to set up all required database objects.

### Prerequisites

- Snowflake account with Cortex features enabled
- `ACCOUNTADMIN` role (or equivalent privileges)
- A warehouse (e.g., `COMPUTE_WH`)

### What the SQL File Does

The script performs the following steps:

#### 1. Create Python Faker UDF

Generates synthetic customer data with realistic names and addresses:

```sql
CREATE OR REPLACE FUNCTION py_faker(locale String, provider String, parameters Variant)
    RETURNS Variant
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.8
    PACKAGES = ('faker', 'simplejson')
    HANDLER = 'fake';
```

#### 2. Generate Master Customer Data (1 Million Records)

Creates the `customer_master_data` table with synthetic customer records:

```sql
CREATE OR REPLACE TABLE customer_master_data AS
SELECT
    uuid_string() id,
    py_faker('en_us','first_name',null)::varchar firstname,
    py_faker('en_us','last_name',null)::varchar lastname,
    py_faker('en_us','street_address',null)::varchar street,
    py_faker('en_us','city',null)::varchar city,
    py_faker('en_us','postcode',null)::varchar zipcode,
    py_faker('en_us','state_abbr',null)::varchar state,
    concat(firstname,' ',lastname) name,
    concat(street,' ',city,' ',state,' ',zipcode) address,
    concat(name,' ',address) full_details
FROM table(generator(rowcount => 1000000));
```

#### 3. Create Vector Embeddings

Vectorizes the `full_details` column using Snowflake Cortex's `e5-base-v2` model:

```sql
CREATE OR REPLACE TABLE customer_master_details_vector AS
SELECT 
    ID,
    snowflake.cortex.embed_text('e5-base-v2', full_details) AS full_details_vector
FROM customer_master_data;
```

#### 4. Generate Test Data with Variations

Creates `new_customer_data` table with intentional data quality issues to simulate real-world scenarios:

- First names: uppercased, abbreviated to initials, or missing
- Last names: uppercased
- Addresses: uppercased
- Cities: uppercased
- States: lowercased or missing
- Zip codes: missing

#### 5. Vectorize Test Data

```sql
CREATE OR REPLACE TABLE new_customer_data_vector AS
SELECT *, snowflake.cortex.embed_text('e5-base-v2', full_details) AS full_details_vector
FROM new_customer_data;
```

#### 6. Similarity Matching Queries

The script includes example queries for:

- **Single record matching** — Find best match for one address
- **Batch matching** — Find best match for all new records using `QUALIFY RANK()`
- **Ad-hoc prompts** — Match user-provided addresses on the fly

### How to Run

1. Open a Snowflake worksheet
2. Copy and paste the contents of `SimilarityMatching.sql`
3. Run the script section by section, or execute all at once
4. Wait for the vector embedding tables to complete (may take several minutes for 1M records)

### Tables Created

| Table | Description |
|-------|-------------|
| `customer_master_data` | 1M synthetic customer records |
| `customer_master_details_vector` | Vector embeddings for master data |
| `new_customer_data` | 10 test records with data quality variations |
| `new_customer_data_vector` | Vector embeddings for test data |

---

## Part 2: Run the Notebook

After building the database with the SQL script, use `AddressSimilarityMatching.ipynb` for interactive similarity matching.

### Prerequisites

- Complete Part 1 (run the SQL script first)
- Create the Cortex Search Service (run once):

```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE customer_support;
USE SCHEMA support;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE CORTEX SEARCH SERVICE customer_master_data_search_svc
  ON full_details
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 day'
  AS (
    SELECT full_details
    FROM customer_master_data
  );
```

### What the Notebook Does

The notebook demonstrates multiple approaches to similarity matching:

#### Section 1: Setup and Data Exploration

- Imports required packages (Streamlit, Pandas, Snowpark)
- Connects to the active Snowflake session
- Previews the master data and counts records

#### Section 2: Address Matching with Cortex Search

Uses the Cortex Search Service for fast semantic search:

```python
from snowflake.core import Root

root = Root(session)
svc = (root
  .databases["customer_support"]
  .schemas["support"]
  .cortex_search_services["customer_master_data_search_svc"]
)

# Search for matching address
resp = svc.search(
  query="Patton 887 Aaron Center WY 15339",
  columns=["full_details"],
  limit=1
).to_json()
```

#### Section 3: Batch Matching

Matches all new customer records against master data and displays results in a DataFrame:

```python
for index, row in new_data.iterrows():
    resp = svc.search(
        query=row["FULL_DETAILS"],
        columns=["full_details"],
        limit=1
    ).to_json()
    # Store matches in DataFrame
```

#### Section 4: Match Quality Verification

Calculates cosine similarity between matched pairs using vector embeddings:

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
| Score | Meaning |
|-------|---------|
| 1.0 | Identical |
| 0.8+ | Strong match |
| 0.5-0.8 | Partial match |
| < 0.5 | Weak match |

#### Section 5: LLM-Powered Match Scoring

Uses Mistral-7B to generate human-readable match scores (1-100):

```sql
SELECT snowflake.cortex.complete('mistral-7b', 
  'How close these two given addresses... Provide a matching score of 1 to 100...'
) AS MATCH_DETAILS
FROM "compared_data";
```

### How to Run the Notebook

1. Upload `AddressSimilarityMatching.ipynb` to Snowflake Notebooks
2. Ensure the Cortex Search Service is created (see Prerequisites above)
3. Run cells sequentially from top to bottom
4. View results in Streamlit widgets and SQL output

---

## Key Snowflake Cortex Functions

| Function | Purpose |
|----------|---------|
| `snowflake.cortex.embed_text()` | Convert text to vector embeddings |
| `VECTOR_COSINE_SIMILARITY()` | Calculate similarity between vectors |
| `snowflake.cortex.complete()` | LLM text generation |
| Cortex Search Service | Fast semantic search over text |

## Use Cases

- **Customer Deduplication** — Identify duplicate records with variations
- **Data Quality** — Validate and score address matching accuracy
- **Record Linkage** — Match records across different data sources
- **Upselling** — Recommend products based on similar customer purchases

## References

- Original concept: [Entity Matching using TF-IDF in Snowpark Python](https://medium.com/snowflake/entity-matching-using-tf-idf-in-snowpark-python-3d1942d4ef19)
- [Snowflake Cortex Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/overview)
- [Cortex Search Service](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)
- [Vector Similarity Functions](https://docs.snowflake.com/en/sql-reference/functions/vector_cosine_similarity)


