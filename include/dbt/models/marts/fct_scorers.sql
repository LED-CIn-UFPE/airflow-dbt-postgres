WITH scorers AS (
  SELECT * FROM {{ ref('stg_scorers') }}
),

dim_player AS (
  SELECT * FROM {{ ref('dim_player') }}
),

dim_team AS (
  SELECT * FROM {{ ref('dim_team') }}
),

dim_competition AS (
  SELECT * FROM {{ ref('dim_competition') }}
)

SELECT
  -- Surrogate Key (SK)
  {{ dbt_utils.generate_surrogate_key(['s.player_id', 't.team_sk', 'c.competition_sk']) }} AS scorer_sk,

  -- FKs para as dimensões
  p.player_sk AS player_sk,
  t.team_sk AS team_sk,
  c.competition_sk AS competition_sk,

  -- Medidas
  s.goals,
  s.assists,
  s.penalties,
  s.played_matches,

  -- Medidas Derivadas
  ROUND(s.goals::NUMERIC / NULLIF(s.played_matches, 0), 2) AS goals_per_match,

  -- Metadados
  s.ingested_at

FROM scorers s
  LEFT JOIN dim_player p ON p.player_id = s.player_id
  LEFT JOIN dim_team t ON t.team_id = s.team_id
  LEFT JOIN dim_competition c ON c.competition_id = s.competition_id