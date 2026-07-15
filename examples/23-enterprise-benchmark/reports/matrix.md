# Throughput Matrix

Every source format x every sink, plus ETL chains exercising all seven
transform activities. Row counts and durations are engine-measured; peak
RSS and average CPU cores are sampled for the single engine process.

## Extract - Load (raw, no transform)

| Case | Pipeline | Rows in | Rows out | Duration | Throughput | Peak RSS | CPU |
|------|----------|--------:|---------:|---------:|-----------:|---------:|----:|
| el01_csv_parquet | `csv -> parquet` | 53,000,000 | 53,000,000 | 90.9s | **583,232/s** | - | - |
| el02_csv_postgres | `csv -> jdbc` | 53,000,000 | 53,000,000 | 797.4s | **66,464/s** | - | - |
| el03_csv_kafka | `csv -> kafka` | 53,000,000 | 53,000,000 | 259.8s | **203,996/s** | - | - |
| el04_csv_s3 | `csv -> s3` | 53,000,000 | 53,000,000 | 95.2s | **556,606/s** | 204 MB | 1.9 |
| el05_csv_null | `csv -> null` | 53,000,000 | 53,000,000 | 34.1s | **1,556,260/s** | 39 MB | 1.4 |
| el06_parquet_postgres | `parquet -> jdbc` | 53,000,000 | 53,000,000 | 899.7s | **58,908/s** | - | - |
| el07_parquet_kafka | `parquet -> kafka` | 53,000,000 | 53,000,000 | 288.0s | **184,031/s** | - | - |
| el08_parquet_s3 | `parquet -> s3` | 53,000,000 | 53,000,000 | 94.7s | **559,479/s** | - | - |
| el09_jsonl_parquet | `jsonlines -> parquet` | 53,000,000 | 53,000,000 | 190.2s | **278,654/s** | - | - |
| el10_jsonl_postgres | `jsonlines -> jdbc` | 53,000,000 | 53,000,000 | 861.3s | **61,538/s** | - | - |
| el11_postgres_parquet | `jdbc -> parquet` | 53,000,000 | 53,000,000 | 117.1s | **452,531/s** | - | - |
| el12_postgres_kafka | `jdbc -> kafka` | 53,000,000 | 53,000,000 | 318.8s | **166,260/s** | - | - |
| el13_postgres_s3 | `jdbc -> s3` | 53,000,000 | 53,000,000 | 123.2s | **430,303/s** | - | - |
| el14_kafka_parquet | `kafka -> parquet` | 53,000,000 | 53,000,000 | 234.6s | **225,910/s** | - | - |
| el15_kafka_postgres | `kafka -> jdbc` | 53,000,000 | 53,000,000 | 859.8s | **61,644/s** | - | - |
| el16_s3_parquet | `s3 -> parquet` | 53,000,000 | 53,000,000 | 111.6s | **475,030/s** | - | - |
| el17_s3_postgres | `s3 -> jdbc` | 53,000,000 | 53,000,000 | 813.6s | **65,140/s** | - | - |

## Extract - Transform - Load (in-flight transforms)

| Case | Pipeline | Rows in | Rows out | Duration | Throughput | Peak RSS | CPU |
|------|----------|--------:|---------:|---------:|-----------:|---------:|----:|
| etl01_csv_filter_parquet | `csv -> filter -> parquet` | 53,000,000 | 38,677,277 | 92.1s | **575,249/s** | - | - |
| etl02_csv_filtermap_parquet | `csv -> filter/map -> parquet` | 53,000,000 | 38,677,277 | 118.9s | **445,682/s** | - | - |
| etl03_csv_clean_postgres | `csv -> validate/map -> jdbc` | 53,000,000 | 50,589,628 | 871.9s | **60,787/s** | - | - |
| etl04_csv_full_s3 | `csv -> filter/validate/map -> s3` | 53,000,000 | 37,942,161 | 170.5s | **310,845/s** | - | - |
| etl05_postgres_map_kafka | `jdbc -> map -> kafka` | 53,000,000 | 53,000,000 | 392.7s | **134,978/s** | - | - |
| etl06_kafka_filter_parquet | `kafka -> filter -> parquet` | 53,000,000 | 38,677,277 | 223.5s | **237,106/s** | - | - |
| etl07_parquet_full_postgres | `parquet -> filter/validate/map -> jdbc` | 53,000,000 | 37,942,161 | 683.7s | **77,522/s** | - | - |
| etl08_csv_schemadrift_parquet | `csv -> schemadrift -> parquet` | 53,000,000 | 53,000,000 | 88.0s | **601,972/s** | - | - |
| etl09_csv_dq_parquet | `csv -> dq -> parquet` | 53,000,000 | 53,000,000 | 178.2s | **297,420/s** | - | - |
| etl10_csv_hwm_parquet | `csv -> hwm -> parquet` | 53,000,000 | 53,000,000 | 92.0s | **576,250/s** | - | - |
| etl11_csv_lookup_parquet | `csv -> lookup/map -> parquet` | 53,000,000 | 53,000,000 | 187.5s | **282,710/s** | - | - |

