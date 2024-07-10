/***
Converted the blog https://medium.com/snowflake/entity-matching-using-tf-idf-in-snowpark-python-3d1942d4ef19
to use CORTEX vector embedding
****/

use role accountadmin;
use database customer_support;
use schema support;

--python faker to create synthetic data
CREATE OR REPLACE FUNCTION py_faker(locale String,provider String,parameters Variant)
    returns Variant
    language python
    runtime_version = 3.8
    packages = ('faker','simplejson')
    handler = 'fake'
as
$$
from faker import Faker
import simplejson as json
def fake(locale,provider,parameters):
    if type(parameters).__name__=='sqlNullWrapper':
        parameters = {}
    fake = Faker(locale=locale)
    return json.loads(json.dumps(fake.format(formatter=provider,**parameters), default=str))
$$;

--lets test it
select
    uuid_string() id,
    py_faker('en_us','first_name',null)::varchar firstname ,
    py_faker('en_us','last_name',null)::varchar lastname,
    py_faker('en_us','street_address',null)::varchar street,
    py_faker('en_us','city',null)::varchar city,
    py_faker('en_us','postcode',null)::varchar zipcode,
    py_faker('en_us','state_abbr',null)::varchar state,
    concat(firstname,' ',lastname) name,
    concat(street,' ',city,' ',state, ' ', zipcode) address,
    concat(name,' ',address) full_details
    from table(generator(rowcount => 10));


--let's create synthetic data
create or replace table customer_master_data as
        select
            uuid_string() id,
            py_faker('en_us','first_name',null)::varchar firstname ,
            py_faker('en_us','last_name',null)::varchar lastname,
            py_faker('en_us','street_address',null)::varchar street,
            py_faker('en_us','city',null)::varchar city,
            py_faker('en_us','postcode',null)::varchar zipcode,
            py_faker('en_us','state_abbr',null)::varchar state,
            concat(firstname,' ',lastname) name,
            concat(street,' ',city,' ',state, ' ', zipcode) address,
            concat(name,' ',address) full_details
        from table(generator(rowcount => 1000000));

select * from customer_master_data
limit 10;

select count(1) from customer_master_data
limit 10;

--lets create a table with vectorized data for the full_detail

create or replace table customer_master_details_vector 
as
select ID,snowflake.cortex.embed_text('e5-base-v2', full_details) as full_details_vector
from customer_master_data
--limit 10
;
select * from customer_master_details_vector
limit 10;

--lets create some sample new data from our existing data to test
create or replace table new_customer_data as
with rnd_factor as
(
    select
        uniform(1,5,random()) as first_name_factor,
        uniform(1,3,random()) as last_name_factor,
        uniform(1,3,random()) as address_factor,
        uniform(1,3,random()) as city_factor,
        uniform(1,5,random()) as state_factor,
        uniform(1,5,random()) as zip_factor,
        *
    from
        customer_master_data
    limit 10   
)
select 
    case  
        when first_name_factor = 1 then upper(left(firstname,1))
        when first_name_factor = 2 then upper(firstname)
        when first_name_factor = 3 then ''
        else firstname
    end as firstname_new,
    
    case  
        when last_name_factor = 2 then upper(lastname)
        else lastname
    end as lastname_new,
    
    case 
        when address_factor = 1 then upper(street)
        else street
    end as street_new,
    
    case 
        when city_factor = 1 then upper(city)
        else city
    end as city_new,

    case 
        when zip_factor = 1 then ''
        else zipcode
    end as zipcode_new,

    case 
        when state_factor = 1 then lower(state)
        when state_factor = 2 then ''
        else state
    end as state_new, 
    
    concat(firstname_new,' ',lastname) as name,
    concat(street_new,' ',' ',' ',state_new, ' ', zipcode_new) as address_new,
    concat(firstname_new,' ',lastname_new,' ',address_new) as full_details
from
    rnd_factor;


select * from new_customer_data
limit 10;

--lets vectorize the full_detial column
create or replace table new_customer_data_vector 
as
select *, snowflake.cortex.embed_text('e5-base-v2', full_details) as full_details_vector
from new_customer_data
--limit 10
;

select * from new_customer_data_vector
limit 10;

--lets see the distance between existing data and new data
-- identical vectors have a cosine similarity of 1, two orthogonal vectors have a similarity of 0, and two opposite vectors have a similarity of -1.

SELECT
   v.id,
   VECTOR_COSINE_SIMILARITY(v.full_details_vector, n.full_details_vector) AS score
FROM 
    customer_master_details_vector v,
    new_customer_data_vector n
ORDER BY 
    score DESC
LIMIT 1
;

SELECT
   v.id,
   m.full_details as org_data,
   n.full_details as new_data,
   VECTOR_COSINE_SIMILARITY(v.full_details_vector, n.full_details_vector) AS score
FROM 
    customer_master_details_vector v inner join
    customer_master_data m on v.id = m.id,
    new_customer_data_vector n
where n.full_details = ' Patton 887 Aaron Center   WY 15339'
ORDER BY 
    score DESC
limit 1
;
--let's find best match for each row of the new data
SELECT
   v.id,
   m.full_details as org_data,
   n.full_details as new_data,
   VECTOR_COSINE_SIMILARITY(v.full_details_vector, n.full_details_vector) AS score
FROM 
    customer_master_details_vector v inner join
    customer_master_data m on v.id = m.id,
    new_customer_data_vector n
QUALIFY RANK() OVER(PARTITION BY n.full_details ORDER BY score desc) = 1;
;

--using prompt to simulate data coming from an UI

SET prompt = 'Lauren Hernandez 927 Melton Course Apt. 068   WA ';
SET prompt = 'Samantha Higgins 31574 RITTER VIA SUITE 500   nj 42546';

select $prompt;

SELECT
    v.id,
    c.full_details,
    $prompt,
    VECTOR_COSINE_SIMILARITY(v.full_details_vector, snowflake.cortex.embed_text('e5-base-v2', $prompt)) AS score
FROM 
customer_master_details_vector v
inner join customer_master_data c
on v.id = c.id,
ORDER BY 
score DESC
LIMIT 1;


-- -- testing alternate prompt scenarios
-- SET prompt = 'Samantha Higgins 31574 RITTER VIA SUITE 500   nj 42546';

-- --vetorize the date bfore you join for higher performance

-- create or replace temp table pv as select snowflake.cortex.embed_text('e5-base-v2', $prompt) AS prompt_vector ;

-- SELECT
--     v.id,
--     $prompt,
--     VECTOR_COSINE_SIMILARITY(v.full_details_vector, pv.prompt_vector) AS score
-- FROM 
-- customer_master_details_vector v,
-- pv 
-- ORDER BY 
-- score DESC
-- LIMIT 1;




--lets see if we find vector on the fly (cartesian join, need XL WH)
with new_customer_data_vector 
as
(
select full_details, snowflake.cortex.embed_text('e5-base-v2', full_details) as full_details_vector
from new_customer_data
)
SELECT
   v.id,
   m.full_details as org_data,
   n.full_details as new_data,
   VECTOR_COSINE_SIMILARITY(v.full_details_vector, n.full_details_vector) AS score
FROM 
    customer_master_details_vector v inner join
    customer_master_data m on v.id = m.id,
    new_customer_data_vector n
QUALIFY RANK() OVER(PARTITION BY n.full_details ORDER BY score desc) = 1;
;

select snowflake.cortex.embed_text('e5-base-v2', null); 


/****
Test/demo our data
****/

--existing data
select * from customer_master_data
limit 10;

--new data
select * from new_customer_data
limit 10;


--let's find best match for each row of the new data
SELECT
   v.id,
   m.full_details as org_data,
   n.full_details as new_data,
   VECTOR_COSINE_SIMILARITY(v.full_details_vector, n.full_details_vector) AS score
FROM 
    customer_master_details_vector v inner join
    customer_master_data m on v.id = m.id,
    new_customer_data_vector n
QUALIFY RANK() OVER(PARTITION BY n.full_details ORDER BY score desc) = 1;
;

