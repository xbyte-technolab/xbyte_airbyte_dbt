{{ config(materialized='table') }}

with __dbt__cte__film_list_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: sys._airbyte_raw_film_list
select
    json_value(_airbyte_data,
    '$."actors"' RETURNING CHAR) as actors,
    json_value(_airbyte_data,
    '$."description"' RETURNING CHAR) as `description`,
    json_value(_airbyte_data,
    '$."category"' RETURNING CHAR) as category,
    _airbyte_ab_id,
    _airbyte_emitted_at,

    CURRENT_TIMESTAMP
 as _airbyte_normalized_at
from sys._airbyte_raw_film_list as table_alias
-- film_list
where 1 = 1
),  __dbt__cte__film_list_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__film_list_ab1
select
    cast(actors as char(1024)) as actors,
    cast(`description` as char(1024)) as `description`,
    cast(category as char(1024)) as category,
    _airbyte_ab_id,
    _airbyte_emitted_at,

    CURRENT_TIMESTAMP
 as _airbyte_normalized_at
from __dbt__cte__film_list_ab1
-- film_list
where 1 = 1
),  __dbt__cte__film_list_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__film_list_ab2
select
    md5(cast(concat(coalesce(cast(actors as char), ''), '-', coalesce(cast(`description` as char), ''), '-', coalesce(cast(category as char), '')) as char)) as _airbyte_film_list_hashid,
    tmp.*
from __dbt__cte__film_list_ab2 tmp
-- film_list
where 1 = 1
)-- Final base SQL model
-- depends_on: __dbt__cte__film_list_ab3
select
    actors,
    `description`,
    category,
    _airbyte_ab_id,
    _airbyte_emitted_at,

    CURRENT_TIMESTAMP
 as _airbyte_normalized_at,
    _airbyte_film_list_hashid
from __dbt__cte__film_list_ab3
-- film_list from sys._airbyte_raw_film_list
where 1 = 1

