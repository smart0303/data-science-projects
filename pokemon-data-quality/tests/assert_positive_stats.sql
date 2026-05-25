-- Singular test: all combat stats must be positive
select id, name, hp, attack, defense, speed
from {{ ref('stg_pokemon') }}
where hp <= 0
   or attack <= 0
   or defense <= 0
   or speed <= 0
