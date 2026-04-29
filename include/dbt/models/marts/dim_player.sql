WITH source AS (
  SELECT DISTINCT ON (player_id)
    player_id,
    player_name,
    nationality,
    position,
    team_id  -- time atual do jogador
  FROM {{ ref('stg_scorers') }}
  ORDER BY player_id
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['player_id']) }} AS player_sk,
  player_id,
  player_name,
  nationality,
  position,
  team_id
FROM source