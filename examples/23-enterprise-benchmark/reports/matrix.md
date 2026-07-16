# Throughput Matrix

Every source format x every sink, plus ETL chains exercising all seven
transform activities. Row counts and durations are engine-measured; peak
RSS and average CPU cores are sampled for the single engine process.

## Extract - Load (raw, no transform)

| Case | Pipeline | Rows in | Rows out | Duration | Throughput | Peak RSS | CPU |
|------|----------|--------:|---------:|---------:|-----------:|---------:|----:|
| el01_csv_parquet | `csv -> parquet` | 53,000,000 | 53,000,000 | 89.0s | **595,613/s** | 148 MB | 2.0 |
| el02_csv_postgres | `csv -> jdbc` | 53,000,000 | 53,000,000 | 866.3s | **61,183/s** | 62 MB | 0.3 |
| el03_csv_kafka | `csv -> kafka` | 53,000,000 | 53,000,000 | 281.8s | **188,047/s** | 66 MB | 1.4 |
| el04_csv_s3 | `csv -> s3` | 53,000,000 | 53,000,000 | 92.0s | **576,338/s** | 204 MB | 1.9 |
| el05_csv_null | `csv -> null` | 53,000,000 | 53,000,000 | 33.5s | **1,580,061/s** | 39 MB | 1.4 |
| el06_parquet_postgres | `parquet -> jdbc` | 53,000,000 | 53,000,000 | 805.2s | **65,824/s** | 234 MB | 0.3 |
| el07_parquet_kafka | `parquet -> kafka` | 53,000,000 | 53,000,000 | 246.4s | **215,074/s** | 245 MB | 1.5 |
| el08_parquet_s3 | `parquet -> s3` | 53,000,000 | 53,000,000 | 91.5s | **579,507/s** | 347 MB | 2.1 |
| el09_jsonl_parquet | `jsonlines -> parquet` | 53,000,000 | 53,000,000 | 180.7s | **293,244/s** | 146 MB | 1.9 |
| el10_jsonl_postgres | `jsonlines -> jdbc` | 53,000,000 | 53,000,000 | 845.0s | **62,725/s** | 73 MB | 0.5 |
| el11_postgres_parquet | `jdbc -> parquet` | 53,000,000 | 53,000,000 | 108.1s | **490,405/s** | 154 MB | 2.1 |
| el12_postgres_kafka | `jdbc -> kafka` | 53,000,000 | 53,000,000 | 293.7s | **180,485/s** | 66 MB | 1.5 |
| el13_postgres_s3 | `jdbc -> s3` | 53,000,000 | 53,000,000 | 113.5s | **466,825/s** | 204 MB | 2.0 |
| el14_kafka_parquet | `kafka -> parquet` | 53,000,000 | 53,000,000 | 217.3s | **243,851/s** | 169 MB | 2.3 |
| el15_kafka_postgres | `kafka -> jdbc` | 53,000,000 | 53,000,000 | 828.2s | **63,997/s** | 86 MB | 0.7 |
| el16_s3_parquet | `s3 -> parquet` | 53,000,000 | 53,000,000 | 117.3s | **451,902/s** | 314 MB | 1.6 |
| el17_s3_postgres | `s3 -> jdbc` | 53,000,000 | 53,000,000 | 853.6s | **62,093/s** | 230 MB | - |

## Extract - Transform - Load (in-flight transforms)

| Case | Pipeline | Rows in | Rows out | Duration | Throughput | Peak RSS | CPU |
|------|----------|--------:|---------:|---------:|-----------:|---------:|----:|
| etl01_csv_filter_parquet | `csv -> filter -> parquet` | 53,000,000 | 38,677,277 | 90.5s | **585,487/s** | 157 MB | 2.0 |
| etl02_csv_filtermap_parquet | `csv -> filter/map -> parquet` | 53,000,000 | 38,677,277 | 116.1s | **456,523/s** | 144 MB | 2.0 |
| etl03_csv_clean_postgres | `csv -> validate/map -> jdbc` | 53,000,000 | 50,589,628 | 1621.7s | **32,681/s** | 61 MB | 0.4 |
| etl04_csv_full_s3 | `csv -> filter/validate/map -> s3` | 53,000,000 | 37,942,161 | 468.4s | **113,153/s** | 181 MB | 1.4 |
| etl05_postgres_map_kafka | `jdbc -> map -> kafka` | 53,000,000 | 53,000,000 | 378.8s | **139,897/s** | 64 MB | 1.5 |
| etl06_kafka_filter_parquet | `kafka -> filter -> parquet` | 53,000,000 | 38,677,277 | 223.5s | **237,130/s** | 153 MB | 2.3 |
| etl07_parquet_full_postgres | `parquet -> filter/validate/map -> jdbc` | 53,000,000 | 37,942,161 | 1708.5s | **31,022/s** | 220 MB | 0.4 |
| etl08_csv_schemadrift_parquet | `csv -> schemadrift -> parquet` | 53,000,000 | 53,000,000 | 87.9s | **603,253/s** | 152 MB | 2.0 |
| etl09_csv_dq_parquet | `csv -> dq -> parquet` | 53,000,000 | 53,000,000 | 176.1s | **300,988/s** | 146 MB | 1.7 |
| etl10_csv_hwm_parquet | `csv -> hwm -> parquet` | 53,000,000 | 53,000,000 | 90.7s | **584,666/s** | 152 MB | 2.0 |
| etl11_csv_lookup_parquet | `csv -> lookup/map -> parquet` | 53,000,000 | 53,000,000 | 182.3s | **290,803/s** | 153 MB | 2.6 |

