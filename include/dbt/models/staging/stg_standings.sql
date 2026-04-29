WITH source AS (
  SELECT payload, ingested_at
  FROM {{ source('raw', 'standings_raw') }}
),

tabela AS (
  SELECT
    jsonb_array_elements((
      SELECT s
      FROM jsonb_array_elements(payload -> 'standings') AS s
      WHERE s ->> 'type' = 'TOTAL'
      LIMIT 1
    ) -> 'table') AS row,
    ingested_at
  FROM source
),

renamed AS (
  SELECT
    (row ->> 'position')::integer                   AS position,
    (row -> 'team' ->> 'id')::integer               AS team_id,
    row -> 'team' ->> 'name'                        AS team_name,
    (row ->> 'playedGames')::integer                AS played_games,
    (row ->> 'won')::integer                        AS won,
    (row ->> 'draw')::integer                       AS draw,
    (row ->> 'lost')::integer                       AS lost,
    (row ->> 'points')::integer                     AS points,
    (row ->> 'goalsFor')::integer                   AS goals_for,
    (row ->> 'goalsAgainst')::integer               AS goals_against,
    (row ->> 'goalDifference')::integer             AS goal_difference,
    ingested_at
  FROM tabela
)

SELECT * FROM renamed