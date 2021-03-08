CREATE TABLE tamasstahl_homework.gold_allviews
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://tamasstahl-ceu-2020/de4/gold_allviews'
    ) AS SELECT 
         article, SUM(views) as total_top_views, MIN(rank) as top_rank, COUNT(date) as ranked_days
         FROM tamasstahl_homework.silver_views
         GROUP BY article
         ORDER BY total_top_views DESC