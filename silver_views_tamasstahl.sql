CREATE TABLE tamasstahl_homework.silver_views
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://tamasstahl-ceu-2020/de4/views_silver'
    ) AS SELECT article, views, rank, date FROM tamasstahl_homework.bronze_views 
    WHERE article is not null;