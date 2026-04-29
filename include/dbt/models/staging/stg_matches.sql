WITH source AS (
  SELECT payload, ingested_at
  FROM {{ source('raw', 'matches') }}
),

matches AS (
  SELECT
    jsonb_array_elements(payload -> 'matches') AS match,
    ingested_at
  FROM source
),
renamed AS (
  SELECT
    -- identificação
    (match ->> 'id')::INTEGER AS match_id,

    -- data
    (match ->> 'utcDate')::TIMESTAMP AS utc_date,
    (match ->> 'utcDate')::TIMESTAMP::DATE AS match_date,

    -- status e rodada
    match ->> 'status' AS match_status,
    (match ->> 'matchday')::INTEGER AS matchday,
    match ->> 'stage' AS stage,

    -- time mandante
    (match -> 'homeTeam' ->> 'id')::INTEGER AS home_team_id,
    match -> 'homeTeam' ->> 'name' AS home_team_name,

    -- time visitante
    (match -> 'awayTeam' ->> 'id')::INTEGER AS away_team_id,
    match -> 'awayTeam' ->> 'name' AS away_team_name,

    -- placar
    (match -> 'score' -> 'fullTime' ->> 'home')::INTEGER AS score_home,
    (match -> 'score' -> 'fullTime' ->> 'away')::INTEGER AS score_away,
    match -> 'score' ->> 'winner' AS winner,

    -- competição
    (match -> 'competition' ->> 'id')::INTEGER AS competition_id,
    match -> 'competition' ->> 'name' AS competition_name,

    -- metadata
    ingested_at

  FROM matches
)

SELECT * FROM renamed 