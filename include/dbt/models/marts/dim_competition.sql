WITH source AS (
  SELECT DISTINCT ON (competition_id)
    competition_id,
    competition_name
  FROM {{ ref('stg_matches') }}
  ORDER BY competition_id
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['competition_id']) }} AS competition_sk,
  competition_id,
  competition_name
FROM source

