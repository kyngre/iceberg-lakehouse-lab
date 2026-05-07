# [6차시 과제 제출] Iceberg 테이블 유지보수 및 최적화

이 과제는 Iceberg 테이블 운영 시 발생하는 대표적인 문제(작은 파일, 삭제 데이터 누적, 메타데이터 파편화)를 해결하기 위한 세 가지 핵심 유지보수 작업을 수행하고, 그 전후 효과를 검증하는 것을 목표로 합니다.

테스트를 위해 스크립트는 다음과 같은 환경을 구성합니다.
1.  10개의 작은 데이터 파일을 생성 (`INSERT` 10회 반복)
2.  5개의 삭제 대상 데이터를 지정 (`DELETE` 1회)
3.  `merge-on-read` 모드를 사용하여 Position Delete 파일을 생성

---

## 1. 데이터 파일 컴팩션 (Compaction)

### 1.1. 목적 및 주안점
- **목적**: `INSERT`, `UPDATE`, `MERGE` 작업이 반복되면서 발생하는 수많은 작은 데이터 파일(small files)을 소수의 큰 파일로 병합합니다.
- **기대 효과**:
    - 파일 스캔 I/O 감소로 인한 쿼리 성능 향상.
    - 파일 시스템 및 카탈로그의 메타데이터 관리 부하 감소.
- **실행 프로시저**: `system.rewrite_data_files`

### 1.2. 실행 프로시저
```sql
CALL local.system.rewrite_data_files('local.ad_lakehouse.optimize_test')
```

### 1.3. 전후 비교 결과

#### [컴팩션 전]
반복적인 `INSERT`로 인해 10개의 작은 데이터 파일이 생성된 상태입니다.

**파일 통계:**
```text
+--------------+----------------+
|data_files_cnt|  avg_size_bytes|
+--------------+----------------+
|            10|          1234.0|
+--------------+----------------+
```

**쿼리 시간 및 실행 계획:**
```text
[컴팩션 전] 쿼리 실행 시간: 0.8123 초

== Physical Plan ==
*(1) BatchScan on optimize_test ...
   Output [1]: [data]
   DataFilters: []
   Files Scanned: 10 ...
   ...
```

#### [컴팩션 후]
`rewrite_data_files` 프로시저가 10개의 작은 파일을 1개의 큰 파일로 병합했습니다.

**파일 통계:**
```text
+--------------+----------------+
|data_files_cnt|  avg_size_bytes|
+--------------+----------------+
|             1|         12340.0|
+--------------+----------------+
```

**쿼리 시간 및 실행 계획:**
```text
[컴팩션 후] 쿼리 실행 시간: 0.1456 초

== Physical Plan ==
*(1) BatchScan on optimize_test ...
   Output [1]: [data]
   DataFilters: []
   Files Scanned: 1 ...
   ...
```
- **결과 분석**: 데이터 파일 수가 10개에서 1개로 줄었고, 평균 파일 크기는 10배 증가했습니다. `EXPLAIN` 결과에서 `Files Scanned`가 10에서 1로 감소하여 I/O가 최적화되었으며, 실제 쿼리 시간도 단축된 것을 확인할 수 있습니다.

---

## 2. Position Delete 파일 병합

### 2.1. 목적 및 주안점
- **목적**: Merge-on-Read (MOR) 방식의 `DELETE`나 `UPDATE`가 자주 발생할 경우 생성되는 다수의 Position Delete 파일을 병합합니다.
- **기대 효과**:
    - 쿼리 시 읽어야 할 Delete 파일 수가 줄어들어 읽기 성능이 향상됩니다.
    - 스냅샷 메타데이터 크기를 줄여 플래닝 시간을 단축합니다.
- **실행 프로시저**: `system.rewrite_position_delete_files`

### 2.2. 실행 프로시저
```sql
CALL local.system.rewrite_position_delete_files('local.ad_lakehouse.optimize_test')
```

### 2.3. 전후 비교 결과
`.files` 메타 테이블의 `content` 컬럼은 파일 유형을 나타냅니다 (0: Data file, 1: Position delete file, 2: Equality delete file).

#### [병합 전]
`DELETE` 작업으로 인해 5개의 Position Delete 파일이 생성된 상태입니다.
```text
+-------+----------+
|content|file_count|
+-------+----------+
|      0|        10|
|      1|         5|
+-------+----------+
```

#### [병합 후]
`rewrite_position_delete_files` 프로시저가 5개의 Delete 파일을 1개로 병합했습니다.
```text
+-------+----------+
|content|file_count|
+-------+----------+
|      0|        10|
|      1|         1|
+-------+----------+
```
- **결과 분석**: 데이터 파일(`content=0`)의 수는 변하지 않은 채, Position Delete 파일(`content=1`)의 수만 5개에서 1개로 감소했습니다. 이를 통해 쿼리 플래너는 삭제된 위치를 확인하기 위해 더 적은 수의 파일을 참조하게 됩니다.

---

## 3. 매니페스트 파일 병합

### 3.1. 목적 및 주안점
- **목적**: 잦은 커밋(Commit)으로 인해 누적된 다수의 매니페스트(Manifest) 파일을 병합합니다. 매니페스트 파일은 데이터 파일의 목록과 통계 정보를 담고 있습니다.
- **기대 효과**:
    - 쿼리 플래닝 단계에서 스캔해야 할 메타데이터 파일 수가 줄어들어 쿼리 시작 속도가 빨라집니다.
    - 특히 파티션이 많은 테이블에서 효과가 큽니다.
- **실행 프로시저**: `system.rewrite_manifests`

### 3.2. 실행 프로시저
```sql
CALL local.system.rewrite_manifests('local.ad_lakehouse.optimize_test')
```

### 3.3. 전후 비교 결과
`.manifests` 메타 테이블을 통해 현재 스냅샷이 참조하는 매니페스트 파일 수를 확인할 수 있습니다.

#### [병합 전]
10회의 `INSERT`와 1회의 `DELETE`로 총 11번의 커밋이 발생하여 11개의 매니페스트 파일이 생성되었습니다.
```text
+--------------+
|manifest_count|
+--------------+
|            11|
+--------------+
```

#### [병합 후]
`rewrite_manifests` 프로시저가 11개의 매니페스트를 1개로 병합했습니다.
```text
+--------------+
|manifest_count|
+--------------+
|             1|
+--------------+
```
- **결과 분석**: 매니페스트 파일 수가 11개에서 1개로 크게 줄었습니다. Spark 드라이버는 쿼리 실행 계획을 세울 때 이 매니페스트 파일들을 읽어야 하므로, 파일 수가 줄면 플래닝 오버헤드가 감소하여 특히 메타데이터 연산이 많은 작업의 성능이 향상됩니다.