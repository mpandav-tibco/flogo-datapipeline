# Throughput Matrix

Every source format x every sink, plus ETL chains exercising all seven
transform activities. Row counts and durations are engine-measured; peak
RSS and average CPU cores are sampled for the single engine process.

## Extract - Load (raw, no transform)

| Case | Pipeline | Rows in | Rows out | Duration | Throughput | Peak RSS | CPU |
|------|----------|--------:|---------:|---------:|-----------:|---------:|----:|
| el01_csv_parquet | `csv -> parquet` | 53,000,000 | 53,000,000 | 89.0s | **595,198/s** | 97 MB | 1.9 |
| el02_csv_postgres | `csv -> jdbc` | 53,000,000 | 53,000,000 | 144.2s | **367,471/s** | 51 MB | 0.8 |
| el03_csv_kafka | `csv -> kafka` | 53,000,000 | 53,000,000 | 286.9s | **184,739/s** | 62 MB | 1.4 |
| el04_csv_s3 | `csv -> s3` | 53,000,000 | 53,000,000 | 92.3s | **574,339/s** | 133 MB | 1.9 |
| el05_csv_null | `csv -> null` | 53,000,000 | 53,000,000 | 34.8s | **1,522,770/s** | 26 MB | 1.2 |
| el06_parquet_postgres | `parquet -> jdbc` | 53,000,000 | 53,000,000 | 121.5s | **436,085/s** | 208 MB | 0.9 |
| el07_parquet_kafka | `parquet -> kafka` | 53,000,000 | 53,000,000 | 266.6s | **198,816/s** | 207 MB | 1.4 |
| el08_parquet_s3 | `parquet -> s3` | 53,000,000 | 53,000,000 | 91.5s | **579,273/s** | 278 MB | 2.0 |
| el09_jsonl_parquet | `jsonlines -> parquet` | 53,000,000 | 53,000,000 | 185.3s | **286,089/s** | 90 MB | 1.7 |
| el10_jsonl_postgres | `jsonlines -> jdbc` | 53,000,000 | 53,000,000 | 196.8s | **269,260/s** | 60 MB | 1.4 |
| el11_postgres_parquet | `jdbc -> parquet` | 53,000,000 | 53,000,000 | 126.1s | **420,248/s** | 114 MB | 2.0 |
| el12_postgres_kafka | `jdbc -> kafka` | 53,000,000 | 53,000,000 | 299.0s | **177,276/s** | 66 MB | 1.6 |
| el13_postgres_s3 | `jdbc -> s3` | 53,000,000 | 53,000,000 | 130.7s | **405,527/s** | 135 MB | 2.0 |
| el14_kafka_parquet | `kafka -> parquet` | 53,000,000 | 53,000,000 | 212.2s | **249,787/s** | 106 MB | 2.2 |
| el15_kafka_postgres | `kafka -> jdbc` | 53,000,000 | 53,000,000 | 234.7s | **225,802/s** | 70 MB | 2.0 |
| el16_s3_parquet | `s3 -> parquet` | 53,000,000 | 53,000,000 | 124.7s | **425,136/s** | 819 MB | 1.8 |
| el17_s3_postgres | `s3 -> jdbc` | 53,000,000 | 53,000,000 | 188.6s | **280,947/s** | 828 MB | 0.7 |

## Extract - Transform - Load (in-flight transforms)

| Case | Pipeline | Rows in | Rows out | Duration | Throughput | Peak RSS | CPU |
|------|----------|--------:|---------:|---------:|-----------:|---------:|----:|
| etl01_csv_filter_parquet | `csv -> filter -> parquet` | 53,000,000 | 38,677,277 | 66.6s | **796,142/s** | 104 MB | 2.2 |
| etl02_csv_filtermap_parquet | `csv -> filter/map -> parquet` | 53,000,000 | 38,677,277 | 68.3s | **776,261/s** | 121 MB | 2.5 |
| etl03_csv_clean_postgres | `csv -> validate/map -> jdbc` | 53,000,000 | 50,589,628 | 112.6s | **470,546/s** | 138 MB | 1.6 |
| etl04_csv_full_s3 | `csv -> filter/validate/map -> s3` | 53,000,000 | 37,942,161 | 77.3s | **685,809/s** | 138 MB | 2.5 |
| etl05_postgres_map_kafka | `jdbc -> map -> kafka` | 53,000,000 | 53,000,000 | 273.5s | **193,758/s** | 93 MB | 2.0 |
| etl06_kafka_filter_parquet | `kafka -> filter -> parquet` | 53,000,000 | 38,677,277 | 210.0s | **252,374/s** | 86 MB | 2.1 |
| etl07_parquet_full_postgres | `parquet -> filter/validate/map -> jdbc` | 53,000,000 | 37,942,161 | 94.6s | **560,058/s** | 281 MB | 1.6 |
| etl08_csv_schemadrift_parquet | `csv -> schemadrift -> parquet` | 53,000,000 | 53,000,000 | 88.3s | **600,097/s** | 118 MB | 2.0 |
| etl09_csv_dq_parquet | `csv -> dq -> parquet` | 53,000,000 | 53,000,000 | 99.0s | **535,289/s** | 125 MB | 3.2 |
| etl10_csv_hwm_parquet | `csv -> hwm -> parquet` | 53,000,000 | 53,000,000 | 88.0s | **602,006/s** | 121 MB | 2.1 |
| etl11_csv_lookup_parquet | `csv -> lookup/map -> parquet` | 53,000,000 | 53,000,000 | 91.3s | **580,421/s** | 119 MB | 2.5 |

