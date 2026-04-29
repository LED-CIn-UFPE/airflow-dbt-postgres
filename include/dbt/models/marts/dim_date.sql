WITH datas AS (
  SELECT DISTINCT match_date AS full_date
  FROM {{ ref('stg_matches') }}
)

SELECT
  -- Surrogate key (SK) formato YYYYMMDD (ex: 20240915)
  TO_CHAR(full_date, 'YYYYMMDD')::INTEGER AS date_id,
  full_date,
  EXTRACT(YEAR  FROM full_date)::INTEGER AS year,
  EXTRACT(MONTH FROM full_date)::INTEGER AS month,
  EXTRACT(DAY   FROM full_date)::INTEGER AS day,
  TO_CHAR(full_date, 'Day') AS day_of_week,
  EXTRACT(DOW FROM full_date)::INTEGER AS day_of_week_num,
  EXTRACT(DOW FROM full_date) IN (0, 6) AS is_weekend

FROM datas
ORDER BY full_date