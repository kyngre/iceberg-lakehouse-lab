```python
import os
from pyspark.sql import SparkSession

# 1. AWS 자격증명 프로필 설정 (터미널에서 -e AWS_PROFILE=metacode 한 것과 동일한 효과)
os.environ["AWS_PROFILE"] = "metacode"

# 2. Spark Session 생성 및 Iceberg/Glue 환경 세팅
spark = (
    SparkSession.builder.appName("Iceberg-Jupyter")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3a://metacode-iceberg-0426/warehouse")
    .getOrCreate()
)

print("Spark Iceberg 환경 세팅 완료!")
```

    Spark Iceberg 환경 세팅 완료!



```python
!/usr/local/spark/bin/spark-submit \
    /home/jovyan/jobs/raw_to_processed_iceberg.py \
    --catalog-mode glue \
    --catalog-name glue_catalog \
    --warehouse s3a://metacode-iceberg-0426/warehouse \
    --raw-path s3a://metacode-iceberg-0426/raw/ad-events
```

# 1. Rollback 실습


```python
# 1) 현재 스냅샷 히스토리 확인
spark.sql("""
    SELECT * FROM glue_catalog.ad_lakehouse.processed_events.history
""").show(truncate=False)
```

    +-----------------------+-------------------+-------------------+-------------------+
    |made_current_at        |snapshot_id        |parent_id          |is_current_ancestor|
    +-----------------------+-------------------+-------------------+-------------------+
    |2026-04-30 11:06:17.271|16564189779592709  |NULL               |true               |
    |2026-04-30 11:11:51.712|1845344206819889774|16564189779592709  |true               |
    |2026-05-02 14:35:49.26 |7047825838959827299|1845344206819889774|true               |
    +-----------------------+-------------------+-------------------+-------------------+
    



```python
# 2) 강제로 잘못된 데이터 INSERT (장애 시나리오) - campaign을 정수형 9999로 수정
spark.sql("""
    INSERT INTO glue_catalog.ad_lakehouse.processed_events (event_id, event_date, uid, campaign, click, conversion, conversion_delay_sec, cost, updated_at) 
    VALUES ('ERROR_999', current_date(), 'user_999', 9999, 1, 0, 0, 100.0, current_timestamp())
""")
```




    DataFrame[]




```python
# 3) 돌아갈 스냅샷 ID 확인 (히스토리에서 확인한 과거 스냅샷 ID 복사)
spark.sql("""
    SELECT committed_at, snapshot_id 
    FROM glue_catalog.ad_lakehouse.processed_events.snapshots 
    ORDER BY committed_at DESC
""").show(truncate=False)
```

    +-----------------------+-------------------+
    |committed_at           |snapshot_id        |
    +-----------------------+-------------------+
    |2026-05-02 14:38:31.263|4147049983052094917|
    |2026-05-02 14:35:49.26 |7047825838959827299|
    |2026-04-30 11:11:51.712|1845344206819889774|
    |2026-04-30 11:06:17.271|16564189779592709  |
    +-----------------------+-------------------+
    



```python
# 4) Rollback 프로시저 실행 (123456789 부분을 복사한 ID로 변경)
spark.sql("""
    CALL glue_catalog.system.rollback_to_snapshot('ad_lakehouse.processed_events', 7047825838959827299)
""")
```




    DataFrame[previous_snapshot_id: bigint, current_snapshot_id: bigint]




```python
# 5) 복구 확인 
spark.sql("""
    SELECT * FROM glue_catalog.ad_lakehouse.processed_events WHERE event_id = 'ERROR_999'
""").show()
```

    +--------+----------+---+--------+-----+----------+--------------------+----+----------+
    |event_id|event_date|uid|campaign|click|conversion|conversion_delay_sec|cost|updated_at|
    +--------+----------+---+--------+-----+----------+--------------------+----+----------+
    +--------+----------+---+--------+-----+----------+--------------------+----+----------+
    


# 2. Expire Snapshots 실습


```python

```


```python
!python -m awscli s3 ls s3://metacode-iceberg-0426/warehouse/ad_lakehouse.db/processed_events/data/ --recursive --human-readable --summarize --profile metacode

```

    2026-04-30 11:11:52   13.4 KiB warehouse/ad_lakehouse.db/processed_events/data/event_date=2026-03-31/00000-5-24cb25e0-c940-4118-a0ac-f120e6021fa4-0-00001.parquet
    2026-04-30 11:11:52    7.4 KiB warehouse/ad_lakehouse.db/processed_events/data/event_date=2026-04-01/00000-5-24cb25e0-c940-4118-a0ac-f120e6021fa4-0-00002.parquet
    2026-05-02 14:38:33    2.4 KiB warehouse/ad_lakehouse.db/processed_events/data/event_date=2026-05-02/00000-2-0bcb62e1-e67b-4820-9bd6-ab2994ea2783-0-00001.parquet
    
    Total Objects: 3
       Total Size: 23.1 KiB



```python
# 2) 시간 안전장치를 풀고 강제로 Expire 처리
spark.sql("""
    CALL glue_catalog.system.expire_snapshots(
      table => 'ad_lakehouse.processed_events', 
      retain_last => 1,
      older_than => TIMESTAMP '2099-12-31 00:00:00'
    )
""").show()
```

    +------------------------+-----------------------------------+-----------------------------------+----------------------------+----------------------------+------------------------------+
    |deleted_data_files_count|deleted_position_delete_files_count|deleted_equality_delete_files_count|deleted_manifest_files_count|deleted_manifest_lists_count|deleted_statistics_files_count|
    +------------------------+-----------------------------------+-----------------------------------+----------------------------+----------------------------+------------------------------+
    |                       1|                                  0|                                  0|                           1|                           3|                             0|
    +------------------------+-----------------------------------+-----------------------------------+----------------------------+----------------------------+------------------------------+
    



```python
# 3) 만료 후 data/ 디렉토리 용량 재확인
!python -m awscli s3 ls s3://metacode-iceberg-0426/warehouse/ad_lakehouse.db/processed_events/data/ --recursive --human-readable --summarize --profile metacode
```

    2026-04-30 11:11:52   13.4 KiB warehouse/ad_lakehouse.db/processed_events/data/event_date=2026-03-31/00000-5-24cb25e0-c940-4118-a0ac-f120e6021fa4-0-00001.parquet
    2026-04-30 11:11:52    7.4 KiB warehouse/ad_lakehouse.db/processed_events/data/event_date=2026-04-01/00000-5-24cb25e0-c940-4118-a0ac-f120e6021fa4-0-00002.parquet
    
    Total Objects: 2
       Total Size: 20.7 KiB


# 3. Orphan File Cleanup 실습


```python
# 1) 가짜 텍스트 파일 생성 후 S3에 .parquet 확장자로 업로드
!echo "dummy orphan data" > dummy.txt
!python -m awscli s3 cp dummy.txt s3://metacode-iceberg-0426/warehouse/ad_lakehouse.db/processed_events/data/dummy_orphan.parquet --profile metacode
```

    upload: ./dummy.txt to s3://metacode-iceberg-0426/warehouse/ad_lakehouse.db/processed_events/data/dummy_orphan.parquet



```python
# 2) 정상적인 안전거리를 둔 고아 파일 정리 (Dry Run)
spark.sql("""
    CALL glue_catalog.system.remove_orphan_files(
      table => 'ad_lakehouse.processed_events', 
      dry_run => true
    )
""").show(truncate=False)
```

    +--------------------+
    |orphan_file_location|
    +--------------------+
    +--------------------+
    



```python
# 3) 실습용 쓰레기 파일 수동 삭제
!python -m awscli s3 rm s3://metacode-iceberg-0426/warehouse/ad_lakehouse.db/processed_events/data/dummy_orphan.parquet --profile metacode
```

    delete: s3://metacode-iceberg-0426/warehouse/ad_lakehouse.db/processed_events/data/dummy_orphan.parquet



```python

```
