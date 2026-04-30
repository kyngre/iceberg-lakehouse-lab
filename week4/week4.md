## [4차시 과제 제출] Silver Layer 테이블 설계 및 검증

### 1. Silver Layer 설계 주안점 및 이유
본 Silver Layer(`processed_events`)는 Raw 데이터(Bronze)를 정제하여 분석에 바로 활용할 수 있는 신뢰도 높은 '단일 진실 공급원(Single Source of Truth)'을 구축하는 것을 목적으로 한 개의 마스터 테이블로 설계했습니다. 구체적인 설계 이유는 다음과 같습니다.

*   **중복 제거 (Deduplication):** 메시지 재전송이나 시스템 지연 등으로 발생할 수 있는 동일 이벤트의 중복 적재를 막기 위해 `event_id`를 기준으로 중복을 제거했습니다.
*   **스키마 표준화 및 타입 캐스팅:** Raw 데이터의 문자열 값을 `click`, `conversion`은 `INT`로, `cost`는 `DOUBLE`로 명시적 형변환을 수행하여 향후 Gold 레이어에서의 집계 연산 효율을 높였습니다.
*   **파생 변수 생성:** 전환된 이벤트에 한정하여 `conversion_delay_sec` (전환 소요 시간) 컬럼을 선제적으로 계산하여 추가했습니다.
*   **자동화 및 정합성 보장 (Upsert):** `MERGE INTO` 구문을 활용하여 파이프라인이 반복적으로 실행되더라도 수작업 없이 자동으로 기존 데이터를 덮어쓰거나(Update) 신규 데이터를 삽입(Insert)하도록 구성했습니다. 이를 통해 데이터 파이프라인의 휴먼 에러를 차단하고 늘 일관된 정합성을 유지합니다.

### 2. 테이블 DDL
Silver 레이어 테이블 생성을 위해 파티셔닝(event_date)과 Iceberg v2 포맷을 적용한 DDL입니다.

```sql
CREATE NAMESPACE IF NOT EXISTS glue_catalog.ad_lakehouse;

CREATE TABLE IF NOT EXISTS glue_catalog.ad_lakehouse.processed_events (
    event_id STRING,
    event_date DATE,
    uid STRING,
    campaign INT,
    click INT,
    conversion INT,
    conversion_delay_sec BIGINT,
    cost DOUBLE,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write',
    'write.target-file-size-bytes' = '134217728'
);
```

### 3. MERGE INTO 쿼리문 (Upsert)
과거 데이터를 포함하여 최신 상태로 병합하기 위해 작성된 쿼리입니다.

```sql
MERGE INTO glue_catalog.ad_lakehouse.processed_events t
USING source_processed_events s
ON t.event_id = s.event_id
WHEN MATCHED THEN
  UPDATE SET
    t.event_date = s.event_date,
    t.uid = s.uid,
    t.campaign = s.campaign,
    t.click = s.click,
    t.conversion = s.conversion,
    t.conversion_delay_sec = s.conversion_delay_sec,
    t.cost = s.cost,
    t.updated_at = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    event_id, event_date, uid, campaign, click, conversion, conversion_delay_sec, cost, updated_at
  )
  VALUES (
    s.event_id, s.event_date, s.uid, s.campaign, s.click, s.conversion, s.conversion_delay_sec, s.cost, s.updated_at
  )
```

### 4. 테이블 검증 쿼리
데이터가 올바르게 적재되고 파티셔닝 되었는지 확인하기 위해 Jupyter 환경에서 수행한 Spark SQL 쿼리입니다.

**[실행 쿼리]**
```python
# 1. Silver 테이블 샘플 데이터 조회
spark.sql("SELECT * FROM glue_catalog.ad_lakehouse.processed_events LIMIT 5").show()

# 2. 파티션(날짜)별 데이터 적재 건수 확인
spark.sql("""
    SELECT event_date, COUNT(*) as cnt 
    FROM glue_catalog.ad_lakehouse.processed_events 
    GROUP BY event_date 
    ORDER BY event_date DESC
""").show()
```
### ![alt text](<Screenshot 2026-04-30 at 8.24.26 PM.png>)5. 스냅샷 비교 결과
Iceberg의 스냅샷 버저닝 기능을 확인하기 위해 메타데이터를 조회한 쿼리입니다.

**[실행 쿼리]**
```python
# Iceberg 스냅샷 이력 조회
spark.sql("""
    SELECT committed_at, snapshot_id, operation, summary 
    FROM glue_catalog.ad_lakehouse.processed_events.snapshots
""").show(truncate=False)
```
![alt text](<Screenshot 2026-04-30 at 8.25.14 PM.png>)

---
***

## [4차시 과제 제출] Gold Layer 테이블 설계 및 검증

### 1. Gold Layer 설계 주안점 및 이유
본 Gold Layer(`campaign_summary`)는 Silver Layer(`processed_events`)의 정제된 데이터를 바탕으로, 마케팅 의사결정에 즉시 활용할 수 있는 '비즈니스 요약(Business Aggregation)' 테이블로 설계되었습니다.

*   **비즈니스 관점의 요약:** 개별 이벤트 단위의 데이터를 마케팅 분석의 핵심 기준인 **'일자별(summary_date)' 및 '캠페인별(campaign)'**로 그룹화하여 일일 성과를 직관적으로 파악할 수 있도록 했습니다.
*   **마케팅 핵심 지표(KPI) 파생 연산:** 단순 합계를 넘어, 파이프라인 단에서 `ctr`(클릭률), `cvr`(전환률), `cpa`(전환당 비용)를 실시간 계산하여 적재함으로써, BI 대시보드나 분석가의 쿼리 부하를 최소화했습니다. 특히 0으로 나누는 에러(Divide by Zero)를 방지하기 위한 `CASE WHEN` 방어 로직을 적용했습니다.
*   **효율적인 멱등성 보장 (Upsert):** `MERGE INTO`의 기준 키를 `summary_date`와 `campaign`으로 설정했습니다. 과거 일자에 지연 도착 데이터(Late Arrival)가 발생하더라도, 복잡한 재작업 없이 이 쿼리 하나로 데이터 정합성을 유지하며 최신 상태로 덮어씁니다.

### 2. 테이블 DDL
비즈니스 요약 데이터를 담기 위한 Gold 테이블 DDL입니다. 데이터 조회 성능 향상을 위해 일자별(`summary_date`) 파티셔닝을 적용했습니다.

```sql
CREATE NAMESPACE IF NOT EXISTS glue_catalog.ad_lakehouse;

CREATE TABLE IF NOT EXISTS glue_catalog.ad_lakehouse.campaign_summary (
    summary_date DATE,
    campaign INT,
    impressions BIGINT,
    clicks BIGINT,
    conversions BIGINT,
    total_cost DOUBLE,
    ctr DOUBLE,
    cvr DOUBLE,
    cpa DOUBLE,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (summary_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write'
);
```

### 3. MERGE INTO 쿼리문 (Upsert)
일정 기간(merge_window_days) 내의 Silver 데이터를 집계하고, 기존 요약 테이블에 병합(Upsert)하는 핵심 쿼리입니다.

```sql
MERGE INTO glue_catalog.ad_lakehouse.campaign_summary t
USING (
  SELECT
    event_date AS summary_date,
    campaign,
    COUNT(*) AS impressions,
    SUM(click) AS clicks,
    SUM(conversion) AS conversions,
    SUM(cost) AS total_cost,
    current_timestamp() AS updated_at
  FROM glue_catalog.ad_lakehouse.processed_events
  WHERE event_date >= current_date() - INTERVAL 3650 DAYS
  GROUP BY event_date, campaign
) s
ON t.summary_date = s.summary_date AND t.campaign = s.campaign
WHEN MATCHED THEN
  UPDATE SET
    t.impressions = s.impressions,
    t.clicks = s.clicks,
    t.conversions = s.conversions,
    t.total_cost = s.total_cost,
    t.ctr = CASE WHEN s.impressions > 0 THEN s.clicks * 100.0 / s.impressions ELSE 0 END,
    t.cvr = CASE WHEN s.clicks > 0 THEN s.conversions * 100.0 / s.clicks ELSE 0 END,
    t.cpa = CASE WHEN s.conversions > 0 THEN s.total_cost / s.conversions ELSE NULL END,
    t.updated_at = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    summary_date, campaign, impressions, clicks, conversions, total_cost, ctr, cvr, cpa, updated_at
  )
  VALUES (
    s.summary_date, s.campaign, s.impressions, s.clicks, s.conversions, s.total_cost,
    CASE WHEN s.impressions > 0 THEN s.clicks * 100.0 / s.impressions ELSE 0 END,
    CASE WHEN s.clicks > 0 THEN s.conversions * 100.0 / s.clicks ELSE 0 END,
    CASE WHEN s.conversions > 0 THEN s.total_cost / s.conversions ELSE NULL END,
    s.updated_at
  )
```

### 4. 테이블 검증 쿼리
캠페인별 집계 지표가 올바르게 수행되었는지 확인하기 위해 Jupyter 환경에서 수행한 Spark SQL 쿼리입니다.

**[실행 쿼리]**
```python
# 1. Gold 테이블 샘플 데이터 확인 (전환수 기준 내림차순 5건)
spark.sql("""
    SELECT * 
    FROM glue_catalog.ad_lakehouse.campaign_summary 
    ORDER BY conversions DESC 
    LIMIT 5
""").show()

# 2. 일자별 캠페인 집계 현황 확인
spark.sql("""
    SELECT summary_date, COUNT(*) as campaign_cnt, SUM(conversions) as total_conv
    FROM glue_catalog.ad_lakehouse.campaign_summary 
    GROUP BY summary_date 
    ORDER BY summary_date DESC
""").show()
```
![alt text](<Screenshot 2026-04-30 at 8.28.17 PM.png>)
### 5. 스냅샷 비교 결과
Gold 테이블에 집계 데이터가 갱신되면서 생성된 Iceberg 스냅샷 버전 관리 이력입니다.

**[실행 쿼리]**
```python
# Gold 테이블 스냅샷 이력 조회
spark.sql("""
    SELECT committed_at, snapshot_id, operation, summary 
    FROM glue_catalog.ad_lakehouse.campaign_summary.snapshots
""").show(truncate=False)
```
![alt text](<Screenshot 2026-04-30 at 8.28.58 PM.png>)
***
