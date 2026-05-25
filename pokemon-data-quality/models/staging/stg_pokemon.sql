with raw as (
    select *
    from {{ ref('pokemon_raw') }}
)

select
    id,
    name,
    type1,
    type2,
    hp,
    attack,
    defense,
    speed,
    generation,
    legendary
from raw
where name is not null
