WITH teams AS (
  SELECT DISTINCT ON (team_id)
    team_id,
    team_name
  FROM {{ ref('stg_standings') }}
  ORDER BY team_id
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['team_id']) }} AS team_sk,
  team_id,
  team_name
FROM teams
