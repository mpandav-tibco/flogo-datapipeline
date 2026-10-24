# Throughput Matrix

Every source format x every sink, plus ETL chains exercising all seven
transform activities. Row counts and durations are engine-measured; peak
RSS and average CPU cores are sampled for the single engine process.

## Extract - Load (raw, no transform)

| Case | Pipeline | Rows in | Rows out | Duration | Throughput | Peak RSS | CPU |
|------|----------|--------:|---------:|---------:|-----------:|---------:|----:|
| el01_csv_parquet | `csv -> parquet` | 53,000,000 | 53,000,000 | 87.9s | **602,752/s** | 96 MB | 1.9 |
| el02_csv_postgres | `csv -> jdbc` | 53,000,000 | 53,000,000 | 149.3s | **355,064/s** | 50 MB | 0.8 |
| el03_csv_kafka | `csv -> kafka` | 53,000,000 | 53,000,000 | 279.6s | **189,563/s** | 63 MB | 1.4 |
| el04_csv_s3 | `csv -> s3` | 53,000,000 | 53,000,000 | 94.1s | **563,219/s** | 134 MB | 1.9 |
| el05_csv_null | `csv -> null` | 53,000,000 | 53,000,000 | 35.1s | **1,509,284/s** | 26 MB | 1.3 |
| el06_parquet_postgres | `parquet -> jdbc` | 53,000,000 | 53,000,000 | 124.4s | **426,192/s** | 205 MB | 0.9 |
| el07_parquet_kafka | `parquet -> kafka` | 53,000,000 | 53,000,000 | 262.1s | **202,242/s** | 213 MB | 1.4 |
| el08_parquet_s3 | `parquet -> s3` | 53,000,000 | 53,000,000 | 93.6s | **566,264/s** | 283 MB | 2.0 |
| el09_jsonl_parquet | `jsonlines -> parquet` | 53,000,000 | 53,000,000 | 184.7s | **286,947/s** | 80 MB | 1.7 |
| el10_jsonl_postgres | `jsonlines -> jdbc` | 53,000,000 | 53,000,000 | 195.0s | **271,831/s** | 58 MB | 1.4 |
| el11_postgres_parquet | `jdbc -> parquet` | 53,000,000 | 53,000,000 | 123.9s | **427,844/s** | 109 MB | 2.1 |
| el12_postgres_kafka | `jdbc -> kafka` | 53,000,000 | 53,000,000 | 299.9s | **176,729/s** | 66 MB | 1.6 |
| el13_postgres_s3 | `jdbc -> s3` | 53,000,000 | 53,000,000 | 130.3s | **406,741/s** | 144 MB | 2.0 |
| el14_kafka_parquet | `kafka -> parquet` | 53,000,000 | 53,000,000 | 213.3s | **248,453/s** | 98 MB | 2.2 |
| el15_kafka_postgres | `kafka -> jdbc` | 53,000,000 | 53,000,000 | 234.6s | **225,894/s** | 70 MB | 2.0 |
| el16_s3_parquet | `s3 -> parquet` | 53,000,000 | 53,000,000 | 127.9s | **414,425/s** | 784 MB | 1.7 |
| el17_s3_postgres | `s3 -> jdbc` | 53,000,000 | 53,000,000 | 149.6s | **354,214/s** | 846 MB | 0.9 |

## Extract - Transform - Load (in-flight transforms)

| Case | Pipeline | Rows in | Rows out | Duration | Throughput | Peak RSS | CPU |
|------|----------|--------:|---------:|---------:|-----------:|---------:|----:|
| etl01_csv_filter_parquet | `csv -> filter -> parquet` | 53,000,000 | 38,677,277 | 67.2s | **788,808/s** | 104 MB | 2.2 |
| etl02_csv_filtermap_parquet | `csv -> filter/map -> parquet` | 53,000,000 | 38,677,277 | 67.6s | **784,511/s** | 116 MB | 2.5 |
| etl03_csv_clean_postgres | `csv -> validate/map -> jdbc` | 53,000,000 | 50,589,628 | 119.1s | **445,094/s** | 144 MB | 1.5 |
| etl04_csv_full_s3 | `csv -> filter/validate/map -> s3` | 53,000,000 | 37,942,161 | 74.0s | **716,633/s** | 139 MB | 2.6 |
| etl05_postgres_map_kafka | `jdbc -> map -> kafka` | 53,000,000 | 53,000,000 | 280.0s | **189,307/s** | 94 MB | 1.9 |
| etl06_kafka_filter_parquet | `kafka -> filter -> parquet` | 53,000,000 | 38,677,277 | 209.5s | **253,016/s** | 114 MB | 2.1 |
| etl07_parquet_full_postgres | `parquet -> filter/validate/map -> jdbc` | 53,000,000 | 37,942,161 | 124.6s | **425,235/s** | 288 MB | 1.2 |
| etl08_csv_schemadrift_parquet | `csv -> schemadrift -> parquet` | 53,000,000 | 53,000,000 | 87.1s | **608,482/s** | 124 MB | 2.1 |
| etl09_csv_dq_parquet | `csv -> dq -> parquet` | 53,000,000 | 53,000,000 | 97.6s | **542,933/s** | 120 MB | 3.2 |
| etl10_csv_hwm_parquet | `csv -> hwm -> parquet` | 53,000,000 | 53,000,000 | 87.7s | **604,650/s** | 117 MB | 2.1 |
| etl11_csv_lookup_parquet | `csv -> lookup/map -> parquet` | 53,000,000 | 53,000,000 | 91.6s | **578,659/s** | 124 MB | 2.5 |

