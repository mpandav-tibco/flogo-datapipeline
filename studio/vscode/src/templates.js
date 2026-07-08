// @ts-check
'use strict'

// Starter templates for the "New from Template" command. Each entry's make(name)
// returns a complete .datapipeline app object. All templates include a timer
// trigger so they run without manual intervention — switch the trigger or add a
// REST trigger on the pipeline tab to suit your deployment.

const PIPELINE_ACTION_REF  = 'github.com/mpandav-tibco/flogo-datapipeline/action/pipeline'
const TIMER_TRIGGER_REF    = 'github.com/project-flogo/contrib/trigger/timer'
const REST_TRIGGER_REF     = 'github.com/project-flogo/contrib/trigger/rest'

/** @param {string} pipelineId @param {number} intervalMs */
function timerTrigger(pipelineId, intervalMs) {
  return {
    id: 'run-timer',
    ref: TIMER_TRIGGER_REF,
    settings: { repeating: true, startDelay: 0, interval: intervalMs },
    handlers: [
      {
        settings: {},
        action: {
          ref: PIPELINE_ACTION_REF,
          settings: { pipelineRef: `res://pipeline:${pipelineId}` },
          input: { params: {} },
        },
      },
    ],
  }
}

/**
 * Wraps a pipeline resource data object into a complete .datapipeline app object.
 * @param {string} name  App / pipeline name (user-supplied)
 * @param {string} pipelineId  Flogo resource id (slug)
 * @param {object} data  Pipeline resource data block
 * @param {number} [intervalMs=86400000]  Timer interval in ms (default: daily)
 * @param {object[]} [properties=[]]  App-level properties
 */
function app(name, pipelineId, data, intervalMs, properties) {
  return {
    name,
    type: 'flogo:app',
    version: '1.0.0',
    appModel: '1.1.1',
    imports: [PIPELINE_ACTION_REF, TIMER_TRIGGER_REF],
    properties: properties || [],
    resources: [
      {
        id: `pipeline:${pipelineId}`,
        data: Object.assign({ name }, data),
      },
    ],
    triggers: [timerTrigger(pipelineId, intervalMs || 86400000)],
  }
}

/** @typedef {{ id: string, label: string, detail: string, make: (name: string) => object }} Template */
/** @type {Template[]} */
const TEMPLATES = [
  // ─────────────────────────────────────────────────────────────────────
  // 1. CSV → PostgreSQL (ETL)
  // ─────────────────────────────────────────────────────────────────────
  {
    id: 'csv-to-postgres',
    label: '$(file-text) CSV → PostgreSQL (ETL)',
    detail: 'Reads a delimited CSV file, filters and maps fields, upserts rows to a PostgreSQL table.',
    make: (name) => app(
      name,
      'csv-to-postgres',
      {
        description: 'Ingests a daily CSV export, normalises fields, and upserts into PostgreSQL.',
        source: {
          type: 'csv',
          path: '/data/orders/${date}.csv',
          hasHeader: true,
          delimiter: ',',
          columns: [
            { name: 'order_id',    type: 'string' },
            { name: 'customer_id', type: 'string' },
            { name: 'product_sku', type: 'string' },
            { name: 'quantity',    type: 'int' },
            { name: 'unit_price',  type: 'decimal', scale: 2 },
            { name: 'currency',    type: 'string' },
            { name: 'status',      type: 'string' },
            { name: 'order_date',  type: 'date', format: '2006-01-02' },
          ],
        },
        transforms: [
          {
            type: 'filter',
            expr: "quantity > 0 AND status != 'CANCELLED'",
          },
          {
            type: 'map',
            fields: {
              order_id:      '=$order_id',
              customer_id:   '=$customer_id',
              product_sku:   '=upper($.product_sku)',
              quantity:      '=$quantity',
              total_amount:  '=multiply($.unit_price, $.quantity)',
              currency_code: '=upper($.currency)',
              status:        '=$status',
              order_date:    '=$order_date',
            },
          },
        ],
        sink: {
          type:        'jdbc',
          driver:      'postgres',
          dsn:         'host=localhost port=5432 dbname=commerce user=pipeline password=secret sslmode=disable',
          table:       'orders_enriched',
          batchSize:   1000,
          onConflict:  'upsert',
          conflictKey: ['order_id'],
        },
        checkpoint: {
          type:            'file',
          path:            '/var/flogo/checkpoints/csv-to-postgres',
          intervalRows:    10000,
          intervalSeconds: 30,
          deleteOnSuccess: false,
        },
        performance: {
          batchSize:           5000,
          transformWorkers:    2,
          sourceBufferBatches: 4,
          sinkBufferBatches:   4,
          maxMemoryMB:         256,
        },
        scaling: { mode: 'single' },
      },
      86400000, // daily
    ),
  },

  // ─────────────────────────────────────────────────────────────────────
  // 2. Parquet on S3 → JDBC (ETL)
  // ─────────────────────────────────────────────────────────────────────
  {
    id: 'parquet-s3-to-jdbc',
    label: '$(cloud-download) Parquet on S3 → JDBC (ETL)',
    detail: 'Reads Parquet files from Amazon S3, validates rows, and bulk-loads into PostgreSQL.',
    make: (name) => app(
      name,
      'parquet-s3-to-jdbc',
      {
        description: 'Reads Parquet data lake files from S3, validates schema and ranges, loads to Postgres.',
        source: {
          type:    'parquet',
          path:    's3://my-data-lake/events/${year}/${month}/${day}/*.parquet',
          columns: ['event_id', 'user_id', 'event_type', 'payload', 'event_ts', 'session_id', 'platform'],
          rowGroupParallelism: 4,
        },
        transforms: [
          {
            type: 'validate',
            rules: [
              { field: 'event_id',   notNull: true, pattern: '^EVT-[0-9A-F]{12}$' },
              { field: 'user_id',    notNull: true },
              { field: 'event_type', notNull: true, in: ['click', 'view', 'purchase', 'signup', 'logout'] },
              { field: 'event_ts',   notNull: true },
            ],
            onViolation: 'route',
            errorSinkType: 'jdbc',
            errorSinkConfig: {
              driver:    'postgres',
              dsn:       'host=localhost port=5432 dbname=analytics user=pipeline password=secret sslmode=disable',
              table:     'events_rejected',
              batchSize: 200,
            },
          },
          {
            type: 'map',
            fields: {
              event_id:   '=$event_id',
              user_id:    '=$user_id',
              event_type: '=$event_type',
              platform:   '=coalesce($.platform, "unknown")',
              session_id: '=$session_id',
              payload:    '=$payload',
              event_ts:   '=$event_ts',
              loaded_at:  '=now()',
            },
          },
        ],
        sink: {
          type:        'jdbc',
          driver:      'postgres',
          dsn:         'host=localhost port=5432 dbname=analytics user=pipeline password=secret sslmode=disable',
          table:       'events_fact',
          batchSize:   2000,
          onConflict:  'upsert',
          conflictKey: ['event_id'],
        },
        checkpoint: {
          type:            'redis',
          addr:            'redis:6379',
          keyPrefix:       'ckpt:parquet-s3',
          intervalRows:    25000,
          intervalSeconds: 60,
          deleteOnSuccess: false,
        },
        performance: {
          batchSize:           10000,
          transformWorkers:    4,
          sourceBufferBatches: 8,
          sinkBufferBatches:   8,
          maxMemoryMB:         512,
          rowGroupParallelism: 4,
        },
        scaling: {
          mode:              'distributed',
          partitionStrategy: 'file',
          coordinator: {
            type:             'postgres',
            dsn:              'host=localhost port=5432 dbname=analytics user=pipeline password=secret sslmode=disable',
            leaseSeconds:     120,
            heartbeatSeconds: 30,
            maxRetries:       3,
          },
        },
      },
      3600000, // hourly
      [
        { name: 's3.region', type: 'string', value: 'us-east-1' },
        { name: 's3.bucket', type: 'string', value: 'my-data-lake' },
      ],
    ),
  },

  // ─────────────────────────────────────────────────────────────────────
  // 3. Database → Database Sync (incremental, JDBC source + JDBC sink)
  // ─────────────────────────────────────────────────────────────────────
  {
    id: 'jdbc-to-jdbc',
    label: '$(database) Database → Database Sync (incremental)',
    detail: 'JDBC source with high-water-mark tracking, JDBC sink with upsert — incremental table replication.',
    make: (name) => app(
      name,
      'jdbc-to-jdbc',
      {
        description: 'Incrementally replicates rows from a source JDBC table to a target JDBC table using a high-water-mark column.',
        source: {
          type:      'jdbc',
          driver:    'postgres',
          dsn:       'host=source-db port=5432 dbname=operational user=reader password=secret sslmode=disable',
          table:     'customers',
          columns:   ['customer_id', 'first_name', 'last_name', 'email', 'phone', 'tier', 'created_at', 'updated_at'],
          where:     "updated_at > '${hwm}'",
          orderKey:  'updated_at',
          batchSize: 5000,
        },
        transforms: [
          {
            type:      'hwm',
            column:    'updated_at',
            valueType: 'datetime',
          },
          {
            type: 'filter',
            expr: "email IS NOT NULL AND email != ''",
          },
          {
            type: 'map',
            fields: {
              customer_id: '=$customer_id',
              first_name:  '=$first_name',
              last_name:   '=$last_name',
              email:       '=lower($.email)',
              phone:       '=$phone',
              tier:        '=coalesce($.tier, "standard")',
              created_at:  '=$created_at',
              updated_at:  '=$updated_at',
              synced_at:   '=now()',
            },
          },
        ],
        sink: {
          type:        'jdbc',
          driver:      'postgres',
          dsn:         'host=target-db port=5432 dbname=warehouse user=writer password=secret sslmode=disable',
          table:       'dim_customers',
          batchSize:   1000,
          onConflict:  'upsert',
          conflictKey: ['customer_id'],
        },
        watermark: {
          type:      'postgres',
          dsn:       'host=target-db port=5432 dbname=warehouse user=writer password=secret sslmode=disable',
          table:     'pipeline_watermarks',
          keyColumn: 'pipeline_name',
          valColumn: 'watermark_value',
        },
        checkpoint: {
          type:            'file',
          path:            '/var/flogo/checkpoints/jdbc-to-jdbc',
          intervalRows:    5000,
          intervalSeconds: 30,
          deleteOnSuccess: false,
        },
        performance: {
          batchSize:           5000,
          transformWorkers:    2,
          sourceBufferBatches: 4,
          sinkBufferBatches:   4,
          maxMemoryMB:         256,
        },
        scaling: { mode: 'single' },
      },
      1800000, // every 30 minutes
    ),
  },

  // ─────────────────────────────────────────────────────────────────────
  // 4. Kafka → JDBC (streaming ETL)
  // ─────────────────────────────────────────────────────────────────────
  {
    id: 'kafka-to-jdbc',
    label: '$(broadcast) Kafka → JDBC (streaming ETL)',
    detail: 'Consumes JSON messages from a Kafka topic, validates and maps fields, writes to a JDBC sink.',
    make: (name) => app(
      name,
      'kafka-to-jdbc',
      {
        description: 'Streams order events from Kafka, validates payload, maps to relational schema, persists to PostgreSQL.',
        source: {
          type:         'kafka',
          brokers:      ['kafka-broker-1:9092', 'kafka-broker-2:9092'],
          topic:        'order-events',
          groupId:      `${name.toLowerCase().replace(/\s+/g, '-')}-consumer`,
          startOffset:  'latest',
          deserializer: 'json',
          mode:         'streaming',
          pollMs:       500,
          batchSize:    500,
        },
        transforms: [
          {
            type: 'validate',
            rules: [
              { field: 'order_id',    notNull: true },
              { field: 'customer_id', notNull: true },
              { field: 'total',       notNull: true, min: 0 },
              { field: 'event_type',  notNull: true, in: ['created', 'updated', 'shipped', 'delivered', 'cancelled'] },
            ],
            onViolation: 'drop',
          },
          {
            type: 'filter',
            expr: "event_type != 'cancelled' OR total > 0",
          },
          {
            type: 'map',
            fields: {
              order_id:     '=$order_id',
              customer_id:  '=$customer_id',
              event_type:   '=$event_type',
              total_amount: '=$total',
              currency:     '=coalesce($.currency, "USD")',
              region:       '=coalesce($.region, "unknown")',
              event_ts:     '=coalesce($.event_ts, now())',
              processed_at: '=now()',
            },
          },
        ],
        sink: {
          type:        'jdbc',
          driver:      'postgres',
          dsn:         'host=localhost port=5432 dbname=orders user=pipeline password=secret sslmode=disable',
          table:       'order_events',
          batchSize:   500,
          onConflict:  'upsert',
          conflictKey: ['order_id', 'event_type'],
        },
        checkpoint: {
          type:            'redis',
          addr:            'redis:6379',
          keyPrefix:       'ckpt:kafka-to-jdbc',
          intervalRows:    10000,
          intervalSeconds: 15,
          deleteOnSuccess: false,
        },
        performance: {
          batchSize:           500,
          transformWorkers:    4,
          sourceBufferBatches: 8,
          sinkBufferBatches:   4,
          maxMemoryMB:         512,
        },
        scaling: { mode: 'single' },
      },
      86400000, // timer unused in streaming mode; pipeline runs continuously
    ),
  },

  // ─────────────────────────────────────────────────────────────────────
  // 5. Mainframe EBCDIC → PostgreSQL (fixed-width)
  // ─────────────────────────────────────────────────────────────────────
  {
    id: 'fixed-width-mainframe',
    label: '$(server) Mainframe EBCDIC → PostgreSQL',
    detail: 'Reads fixed-width EBCDIC files from mainframe output, filters, validates, and loads to PostgreSQL.',
    make: (name) => app(
      name,
      'fixed-width-mainframe',
      {
        description: 'Ingests IBM mainframe EBCDIC-037 fixed-width ledger files, applies business rules, and persists to PostgreSQL.',
        source: {
          type:         'fixedwidth',
          path:         '/mnt/mainframe/ledger/${date}.dat',
          encoding:     'ebcdic-037',
          recordLength: 150,
          skipLines:    1,
          columns: [
            { name: 'account_id',   start: 1,   length: 12, type: 'string',  trim: true },
            { name: 'branch_code',  start: 13,  length: 4,  type: 'string',  trim: true },
            { name: 'txn_code',     start: 17,  length: 3,  type: 'string',  trim: true },
            { name: 'amount',       start: 20,  length: 15, type: 'decimal', impliedDecimal: 2, trim: true },
            { name: 'debit_credit', start: 35,  length: 1,  type: 'string' },
            { name: 'post_date',    start: 36,  length: 8,  type: 'date',    format: '20060102' },
            { name: 'value_date',   start: 44,  length: 8,  type: 'date',    format: '20060102' },
            { name: 'description',  start: 52,  length: 40, type: 'string',  trim: true },
            { name: 'ref_number',   start: 92,  length: 20, type: 'string',  trim: true },
            { name: 'currency',     start: 112, length: 3,  type: 'string',  trim: true },
            { name: 'status_flag',  start: 115, length: 1,  type: 'string' },
          ],
        },
        transforms: [
          {
            type: 'filter',
            expr: "status_flag = 'A' AND amount IS NOT NULL AND amount != 0",
          },
          {
            type: 'validate',
            rules: [
              { field: 'account_id',   notNull: true, pattern: '^[0-9A-Z]{1,12}$' },
              { field: 'branch_code',  notNull: true },
              { field: 'txn_code',     notNull: true },
              { field: 'amount',       notNull: true, min: 0 },
              { field: 'debit_credit', notNull: true, in: ['D', 'C'] },
              { field: 'post_date',    notNull: true },
              { field: 'currency',     notNull: true },
            ],
            onViolation: 'route',
            errorSinkType: 'jdbc',
            errorSinkConfig: {
              driver:    'postgres',
              dsn:       'host=localhost port=5432 dbname=banking user=pipeline password=secret sslmode=disable',
              table:     'ledger_rejects',
              batchSize: 100,
            },
          },
          {
            type: 'map',
            fields: {
              account_id:    '=$account_id',
              branch_code:   '=$branch_code',
              txn_code:      '=$txn_code',
              amount:        '=$amount',
              debit_credit:  '=$debit_credit',
              signed_amount: '=if(eq($.debit_credit, "D"), negate($.amount), $.amount)',
              post_date:     '=$post_date',
              value_date:    '=$value_date',
              description:   '=$description',
              ref_number:    '=$ref_number',
              currency:      '=$currency',
              loaded_at:     '=now()',
            },
          },
        ],
        sink: {
          type:       'jdbc',
          driver:     'postgres',
          dsn:        'host=localhost port=5432 dbname=banking user=pipeline password=secret sslmode=disable',
          table:      'ledger_entries',
          batchSize:  2000,
          onConflict: 'insert',
        },
        checkpoint: {
          type:            'file',
          path:            '/var/flogo/checkpoints/mainframe-ledger',
          intervalRows:    20000,
          intervalSeconds: 60,
          deleteOnSuccess: false,
        },
        performance: {
          batchSize:           10000,
          transformWorkers:    2,
          sourceBufferBatches: 4,
          sinkBufferBatches:   4,
          maxMemoryMB:         256,
        },
        scaling: { mode: 'single' },
      },
      86400000, // daily batch
    ),
  },

  // ─────────────────────────────────────────────────────────────────────
  // 6. Extract & Load only (EL — no transform stage)
  // ─────────────────────────────────────────────────────────────────────
  {
    id: 'el-only',
    label: '$(arrow-right) Extract & Load only (EL)',
    detail: 'CSV source straight to JDBC sink — no transforms. Use when the target schema mirrors the source exactly.',
    make: (name) => app(
      name,
      'el-only',
      {
        description: 'Pure Extract & Load: reads a CSV file and writes every row directly to a JDBC table without any transformation.',
        mode: 'el',
        source: {
          type:      'csv',
          path:      '/data/raw/${date}/import.csv',
          hasHeader: true,
          delimiter: ',',
          columns: [
            { name: 'id',          type: 'string' },
            { name: 'source_id',   type: 'string' },
            { name: 'category',    type: 'string' },
            { name: 'value',       type: 'decimal', scale: 4 },
            { name: 'tags',        type: 'string' },
            { name: 'region',      type: 'string' },
            { name: 'recorded_at', type: 'date', format: '2006-01-02T15:04:05Z' },
          ],
        },
        transforms: [],
        sink: {
          type:        'jdbc',
          driver:      'postgres',
          dsn:         'host=localhost port=5432 dbname=staging user=loader password=secret sslmode=disable',
          table:       'raw_import',
          batchSize:   2000,
          onConflict:  'upsert',
          conflictKey: ['id'],
        },
        checkpoint: {
          type:            'file',
          path:            '/var/flogo/checkpoints/el-only',
          intervalRows:    10000,
          intervalSeconds: 30,
          deleteOnSuccess: true,
        },
        performance: {
          batchSize:           5000,
          transformWorkers:    1,
          sourceBufferBatches: 4,
          sinkBufferBatches:   4,
          maxMemoryMB:         128,
        },
        scaling: { mode: 'single' },
      },
      86400000, // daily
    ),
  },
]

module.exports = { TEMPLATES }
