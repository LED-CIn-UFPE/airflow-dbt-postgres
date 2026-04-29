WITH source AS  (
    SELECT payload, ingested_at
    FROM {{ source('raw', 'scorers_raw') }}
),

scorers AS (
  SELECT
    jsonb_array_elements(payload -> 'scorers') AS scorer,
    (payload -> 'competition' ->> 'id')::integer AS competition_id,
    ingested_at
  FROM source
),

renamed AS (
  SELECT
    (scorer -> 'player' ->> 'id')::integer AS player_id,
    scorer -> 'player' ->> 'name'          AS player_name,
    scorer -> 'player' ->> 'nationality'   AS nationality,
    scorer -> 'player' ->> 'position'      AS position,
    (scorer -> 'team' ->> 'id')::integer   AS team_id,
    scorer -> 'team' ->> 'name'            AS team_name,
    (scorer ->> 'goals')::integer          AS goals,
    (scorer ->> 'assists')::integer        AS assists,
    (scorer ->> 'penalties')::integer      AS penalties,
    (scorer ->> 'playedMatches')::integer  AS played_matches,
    competition_id,
    ingested_at
  FROM scorers
)

SELECT * FROM renamed