WITH partidas AS (
  SELECT * FROM {{ ref('stg_matches') }}
),

dim_date AS (
  SELECT * FROM {{ ref('dim_date') }}
),

dim_team_home AS (
  SELECT * FROM {{ ref('dim_team') }}
),

dim_team_away AS (
  SELECT * FROM {{ ref('dim_team') }}
),

dim_competition AS (
  SELECT * FROM {{ ref('dim_competition') }}
)

SELECT
  -- Natural Key (NK)
  m.match_id AS match_sk,

  -- Surrogate Key (SK)
  {{dbt_utils.generate_surrogate_key([
    'm.match_id',
    'th.team_sk',
    'ta.team_sk',
    'c.competition_sk'
  ])}} AS match_surrogate_sk,

  -- Dimensoes (FK)
  d.date_id as date_sk,
  th.team_sk AS home_team_sk,
  ta.team_sk AS away_team_sk,
  c.competition_sk   AS competition_sk,

  -- Medidas
  m.score_home,
  m.score_away,
  m.score_home + m.score_away                           AS total_goals,
  m.score_home - m.score_away                           AS goal_diff,

  -- Dimensões Degeneradas
  m.matchday,
  m.match_status,
  m.stage,
  m.winner,

  -- Flags
  m.winner = 'HOME_TEAM'                                AS home_win,
  m.winner = 'AWAY_TEAM'                                AS away_win,
  m.winner = 'DRAW'                                     AS is_draw,

  -- Metadados
  m.ingested_at

FROM partidas m
  LEFT JOIN dim_date        d  ON d.full_date = m.match_date
  LEFT JOIN dim_team_home   th ON th.team_id  = m.home_team_id
  LEFT JOIN dim_team_away   ta ON ta.team_id  = m.away_team_id
  LEFT JOIN dim_competition c  ON c.competition_id = m.competition_id
WHERE m.match_status = 'FINISHED'