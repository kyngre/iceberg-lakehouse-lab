"""
Iceberg 테이블 유지보수(최적화) 과제 수행 스크립트.
1. rewrite_data_files (컴팩션)
2. rewrite_position_delete_files (MOR 위치 삭제 파일 병합)
3. rewrite_manifests (매니페스트 파일 병합)
"""

import os
import time
import argparse
from pyspark.sql import SparkSession

def build_spark(catalog_mode, catalog_name, warehouse):
    builder = SparkSession.builder.appName("IcebergOptimizationLab") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")

    if catalog_mode == "glue":
        builder = builder \
            .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
    else:
        builder = builder \
            .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop") \
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)

    return builder.getOrCreate()

def measure_query(spark, query, label=""):
    start_time = time.time()
    spark.sql(query).collect()
    elapsed = time.time() - start_time
    print(f"[{label}] 쿼리 실행 시간: {elapsed:.4f} 초")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog-mode", choices=["local", "glue"], default="local")
    parser.add_argument("--catalog-name", default="local")
    parser.add_argument("--warehouse", default="/home/jovyan/warehouse")
    args = parser.parse_args()

    spark = build_spark(args.catalog_mode, args.catalog_name, args.warehouse)
    table_name = f"{args.catalog_name}.ad_lakehouse.optimize_test"

    # =====================================================================
    # 0. 테스트 테이블 준비 (Small files & Deletes 환경 구성)
    # =====================================================================
    print(">>> 0. 테스트용 테이블 및 데이터 세팅 중...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog_name}.ad_lakehouse")
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    
    # MOR(Merge-On-Read) 모드로 테이블 생성
    spark.sql(f"""
        CREATE TABLE {table_name} (id INT, data STRING)
        USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read'
        )
    """)
    
    # 다수의 작은 파일 생성 (반복 Insert)
    for i in range(1, 11):
        spark.sql(f"INSERT INTO {table_name} VALUES ({i}, 'val_{i}')")
        
    # Delete File 생성을 위한 삭제 연산
    spark.sql(f"DELETE FROM {table_name} WHERE id % 2 = 0")

    # =====================================================================
    # 1. 컴팩션 전후 파일 통계 비교 (rewrite_data_files)
    # =====================================================================
    print("\n" + "="*60)
    print(" 1. 컴팩션 (Data Files) 전후 비교")
    print("="*60)
    
    # [전] 파일 개수 및 평균 크기 (content = 0 은 데이터 파일)
    print("--- [컴팩션 전] 파일 통계 ---")
    spark.sql(f"SELECT COUNT(*) as data_files_cnt, AVG(file_size_in_bytes) as avg_size_bytes FROM {table_name}.files WHERE content = 0").show()
    measure_query(spark, f"SELECT count(data) FROM {table_name}", "컴팩션 전")
    spark.sql(f"SELECT count(data) FROM {table_name}").explain()
    
    # 프로시저 실행
    print("--- 컴팩션 실행 중... ---")
    spark.sql(f"CALL {args.catalog_name}.system.rewrite_data_files('{table_name}')").show(truncate=False)
    
    # [후] 파일 개수 및 평균 크기
    print("--- [컴팩션 후] 파일 통계 ---")
    spark.sql(f"SELECT COUNT(*) as data_files_cnt, AVG(file_size_in_bytes) as avg_size_bytes FROM {table_name}.files WHERE content = 0").show()
    measure_query(spark, f"SELECT count(data) FROM {table_name}", "컴팩션 후")
    spark.sql(f"SELECT count(data) FROM {table_name}").explain()

    # =====================================================================
    # 2. MOR 테이블의 rewrite_position_delete_files 전후
    # =====================================================================
    print("\n" + "="*60)
    print(" 2. Position Delete Files 병합 전후 비교")
    print("="*60)
    
    # [전] content 컬럼 분포 (0: Data, 1: Position Delete, 2: Equality Delete)
    print("--- [병합 전] content 컬럼 분포 ---")
    spark.sql(f"SELECT content, COUNT(*) as file_count FROM {table_name}.files GROUP BY content").show()
    
    # Delete 프로시저 실행
    print("--- Delete File 병합 실행 중... ---")
    spark.sql(f"CALL {args.catalog_name}.system.rewrite_position_delete_files('{table_name}')").show(truncate=False)
    
    # [후] content 컬럼 분포
    print("--- [병합 후] content 컬럼 분포 ---")
    spark.sql(f"SELECT content, COUNT(*) as file_count FROM {table_name}.files GROUP BY content").show()

    # =====================================================================
    # 3. rewrite_manifests 전후 manifest 개수 비교
    # =====================================================================
    print("\n" + "="*60)
    print(" 3. 매니페스트 파일 병합 전후 비교")
    print("="*60)
    
    print("--- [병합 전] 매니페스트 개수 ---")
    spark.sql(f"SELECT COUNT(*) as manifest_count FROM {table_name}.manifests").show()
    
    print("--- Manifest 병합 실행 중... ---")
    spark.sql(f"CALL {args.catalog_name}.system.rewrite_manifests('{table_name}')").show(truncate=False)
    
    print("--- [병합 후] 매니페스트 개수 ---")
    spark.sql(f"SELECT COUNT(*) as manifest_count FROM {table_name}.manifests").show()

if __name__ == "__main__":
    main()