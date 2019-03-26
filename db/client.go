package pgdb

// This package is utilizing functions from:
// https://github.com/timescale/prometheus-postgresql-adapter/blob/master/postgresql/client.go

import (
    "os"
    "fmt"
    "time"
    "sort"
    "strings"
    "context"
    "database/sql"
    "encoding/json"

    _ "github.com/lib/pq"

    "github.com/prometheus/client_golang/prometheus"

    "github.com/arslanm/kafka-timescaledb-adapter/log"
    "github.com/arslanm/kafka-timescaledb-adapter/util"
)

// Config for the database
type Config struct {
    host                      string
    port                      int
    username                  string
    password                  string
    database                  string
    table                     string
    copyTable                 string
    maxOpenConns              int
    maxIdleConns              int
    maxConnLifetime           time.Duration
    pgPrometheusNormalize     bool
    pgPrometheusChunkInterval time.Duration
    useTimescaleDb            bool
}

const (

    DEFAULT_PG_HOST               = "localhost"
    DEFAULT_PG_PORT               = 5432
    DEFAULT_PG_USERNAME           = "prometheus"
    DEFAULT_PG_PASSWORD           = "prometheus"
    DEFAULT_PG_DATABASE           = "prometheus"
    DEFAULT_PG_TABLE              = "metrics"
    DEFAULT_PG_COPY_TABLE         = ""
    DEFAULT_PG_MAX_OPEN_CONNS     = 10
    DEFAULT_PG_MAX_IDLE_CONNS     = 2
    DEFAULT_PG_MAX_CONN_LIFETIME  = "1h"
    DEFAULT_PG_CHUNK_INTERVAL     = "12h"
    DEFAULT_PG_NORMALIZE          = true
    DEFAULT_PG_USE_TIMESCALEDB    = true

    sqlCreateTmpTable = "CREATE TEMPORARY TABLE IF NOT EXISTS %s_tmp_%d(sample prom_sample) ON COMMIT DELETE ROWS;"
    sqlCopyTable      = "COPY \"%s\" FROM STDIN"
    sqlInsertLabels   = "INSERT INTO %s_labels (metric_name, labels) SELECT tmp.prom_name, tmp.prom_labels FROM (SELECT prom_time(sample), prom_value(sample), prom_name(sample), prom_labels(sample) FROM %s_tmp_%d) tmp LEFT JOIN %s_labels l ON tmp.prom_name=l.metric_name AND tmp.prom_labels=l.labels WHERE l.metric_name IS NULL ON CONFLICT (metric_name, labels) DO NOTHING;"
    sqlInsertValues   = "INSERT INTO %s_values SELECT tmp.prom_time, tmp.prom_value, l.id FROM (SELECT prom_time(sample), prom_value(sample), prom_name(sample), prom_labels(sample) FROM %s_tmp_%d) tmp INNER JOIN %s_labels l on tmp.prom_name=l.metric_name AND  tmp.prom_labels=l.labels;"
)

var (
    receivedMetrics = prometheus.NewCounter(
        prometheus.CounterOpts{
            Namespace : "kafka_timescale_adapter",
            Name      : "received_metrics_total",
            Help      : "Total number of received metrics.",
        },
    )

    sentMetrics = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace : "kafka_timescale_adapter",
            Name      : "sent_metrics_total",
            Help      : "Total number of metrics sent to remote storage.",
        },
        []string{"remote"},
    )

    failedMetrics = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace : "kafka_timescale_adapter",
            Name      : "failed_metrics_total",
            Help      : "Total number of metrics which failed on send to remote storage.",
        },
        []string{"remote"},
    )

    sentDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Namespace : "kafka_timescale_adapter",
            Name      : "sent_batch_duration_seconds",
            Help      : "Duration of batch send to remote storage.",
            Buckets   : prometheus.DefBuckets,
        },
        []string{"remote"},
    )

    createTmpTableStmt *sql.Stmt
    copyTableUniqId int64
)

func GetConfig(cfg *Config) *Config {

    cfg.host = util.GetEnvWithDefault("PG_HOST", DEFAULT_PG_HOST)
    cfg.port = util.GetEnvWithDefaultInt("PG_PORT", DEFAULT_PG_PORT)
    cfg.database = util.GetEnvWithDefault("PG_DATABASE", DEFAULT_PG_DATABASE)
    cfg.username = util.GetEnvWithDefault("PG_USERNAME", DEFAULT_PG_USERNAME)
    cfg.password = util.GetEnvWithDefault("PG_PASSWORD", DEFAULT_PG_PASSWORD)
    cfg.table = util.GetEnvWithDefault("PG_TABLE", DEFAULT_PG_TABLE)
    cfg.copyTable = util.GetEnvWithDefault("PG_COPY_TABLE", DEFAULT_PG_COPY_TABLE)
    cfg.maxOpenConns = util.GetEnvWithDefaultInt("PG_MAX_OPEN_CONNS", DEFAULT_PG_MAX_OPEN_CONNS)
    cfg.maxIdleConns = util.GetEnvWithDefaultInt("PG_MAX_IDLE_CONNS", DEFAULT_PG_MAX_IDLE_CONNS)
    cfg.maxConnLifetime = util.GetEnvWithDefaultDuration("PG_MAX_CONN_LIFETIME", DEFAULT_PG_MAX_CONN_LIFETIME)
    cfg.pgPrometheusNormalize = util.GetEnvWithDefaultBool("PG_NORMALIZE", DEFAULT_PG_NORMALIZE)
    cfg.pgPrometheusChunkInterval = util.GetEnvWithDefaultDuration("PG_CHUNK_INTERVAL", DEFAULT_PG_CHUNK_INTERVAL)
    cfg.useTimescaleDb = util.GetEnvWithDefaultBool("PG_USE_TIMESCALEDB", DEFAULT_PG_USE_TIMESCALEDB)

    return cfg
}

type Client struct {
    DB         *sql.DB
    cfg        *Config
    Whitelist  *util.Whitelist
}

func InitPromMetrics() {
    prometheus.MustRegister(receivedMetrics)
    prometheus.MustRegister(sentMetrics)
    prometheus.MustRegister(failedMetrics)
    prometheus.MustRegister(sentDuration)
}

func NewClient(cfg *Config, wl *util.Whitelist) *Client {
    connStr := fmt.Sprintf("host=%v port=%v user=%v dbname=%v password='%v' sslmode=disable connect_timeout=10",
        cfg.host, cfg.port, cfg.username, cfg.database, cfg.password)

    db, err := sql.Open("postgres", connStr)

    if err != nil {
        log.Error("error", err)
        os.Exit(1)
    }

    db.SetMaxOpenConns(cfg.maxOpenConns)
    db.SetMaxIdleConns(cfg.maxIdleConns)
    db.SetConnMaxLifetime(cfg.maxConnLifetime)

    client := &Client{DB: db, cfg: cfg, Whitelist: wl}

    err = client.setupPgPrometheus()
    if err != nil {
        log.Error("error", err)
        os.Exit(1)
    }

    copyTableUniqId = time.Now().UnixNano() / int64(time.Millisecond)
    createTmpTableStmt, err = db.Prepare(fmt.Sprintf(sqlCreateTmpTable, cfg.table, copyTableUniqId))
    if err != nil {
        log.Error("msg", "Error on preparing create tmp table statement", "error", err)
        os.Exit(1)
    }

    InitPromMetrics()

    return client
}

func (c *Client) setupPgPrometheus() error {
    tx, err := c.DB.Begin()
    if err != nil {
        return err
    }

    defer tx.Rollback()

    _, err = tx.Exec("CREATE EXTENSION IF NOT EXISTS pg_prometheus")
    if err != nil {
        return err
    }

    if c.cfg.useTimescaleDb {
        _, err = tx.Exec("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
    }
    if err != nil {
        log.Info("msg", "Could not enable TimescaleDB extension", "error", err)
    }

    var rows *sql.Rows
    rows, err = tx.Query("SELECT create_prometheus_table($1, normalized_tables => $2, chunk_time_interval => $3,  use_timescaledb=> $4)",
        c.cfg.table, c.cfg.pgPrometheusNormalize, c.cfg.pgPrometheusChunkInterval.String(), c.cfg.useTimescaleDb)

    if err != nil {
        if strings.Contains(err.Error(), "already exists") {
            return nil
        }
        return err
    }
    rows.Close()

    err = tx.Commit()

    if err != nil {
        return err
    }

    log.Info("msg", "Initialized pg_prometheus extension")
    return nil
}

func parseJSONMetric(m string) (interface {}, error) {
    var f interface{}
    err := json.Unmarshal([]byte(m), &f)
    return f, err
}

func (c *Client) Insert(ctx context.Context, metrics []string) (int, error) {
    tx, err := c.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
    if err != nil {
        log.Error("msg", "Error on Begin when writing samples", "error", err)
        return 0, err
    }

    defer tx.Rollback()

    _, err = tx.Stmt(createTmpTableStmt).Exec()
    if err != nil {
        log.Error("msg", "Error executing create tmp table", "error", err)
        return 0, err
    }

    var copyTable string
    if len(c.cfg.copyTable) > 0 {
        copyTable = c.cfg.copyTable
    } else if c.cfg.pgPrometheusNormalize {
        copyTable = fmt.Sprintf("%s_tmp_%d", c.cfg.table, copyTableUniqId)
    } else {
        copyTable = fmt.Sprintf("%s_samples", c.cfg.table)
    }

    copyStmt, err := tx.Prepare(fmt.Sprintf(sqlCopyTable, copyTable))
    if err != nil {
        log.Error("msg", "Error on COPY prepare", "error", err)
        return 0, err
    }

    sentCount := 0
    for _, metric := range metrics {
        jsonMsg, err := parseJSONMetric(metric)
        if err != nil {
            log.Error("msg", "Can't parse JSON metric", "error", err)
            continue
        }

        m, ok := jsonMsg.(map[string]interface{})
        if !ok {
            log.Error("msg", "Can't find metric object")
            continue
        }

        name := fmt.Sprintf("%v", m["name"])
        if c.Whitelist != nil {
            if !c.Whitelist.IsWhitelisted(name) {
                continue
            }
        }

        timestamp := fmt.Sprintf("%v", m["timestamp"])
        value := fmt.Sprintf("%v", m["value"])

        labelMap := m["labels"].(map[string]interface{})
        labelStrings := make([]string, 0, len(labelMap))

        for l, v := range labelMap {
            if l == "__name__" {
                name = fmt.Sprintf("%s", v)
                continue
            }
            if l != name {
                labelStrings = append(labelStrings, fmt.Sprintf("%s=%q", l, v))
            }
        }

        sort.Strings(labelStrings)
        labels := fmt.Sprintf("{%s}", strings.Join(labelStrings, ","))

        ts, err := time.Parse(time.RFC3339, timestamp)
        if err != nil {
            log.Error("error", "Can't parse timestamp -- ignoring metric", "error", err)
            continue
        }

        line := fmt.Sprintf("%s%s %s %v", name, labels, value, ts.UnixNano() / 1000000)

        _, err = copyStmt.Exec(line)
        if err != nil {
            log.Error("msg", "Error executing COPY statement", "stmt", line, "error", err)
            return 0, err
        }

        sentCount += 1
    }

    _, err = copyStmt.Exec()
    if err != nil {
        log.Error("msg", "Error executing COPY statement", "error", err)
        return 0, err
    }

    if copyTable == fmt.Sprintf("%s_tmp_%d", c.cfg.table, copyTableUniqId) {
        stmtLabels, err := tx.Prepare(fmt.Sprintf(sqlInsertLabels, c.cfg.table, c.cfg.table, copyTableUniqId, c.cfg.table))
        if err != nil {
            log.Error("msg", "Error on preparing labels statement", "error", err)
            return 0, err
        }
        _, err = stmtLabels.Exec()
        if err != nil {
            log.Error("msg", "Error executing labels statement", "error", err)
            return 0, err
        }

        stmtValues, err := tx.Prepare(fmt.Sprintf(sqlInsertValues, c.cfg.table, c.cfg.table, copyTableUniqId, c.cfg.table))
        if err != nil {
            log.Error("msg", "Error on preparing values statement", "error", err)
            return 0, err
        }
        _, err = stmtValues.Exec()
        if err != nil {
            log.Error("msg", "Error executing values statement", "error", err)
            return 0, err
        }

        err = stmtLabels.Close()
        if err != nil {
            log.Error("msg", "Error on closing labels statement", "error", err)
            return 0, err
        }

        err = stmtValues.Close()
        if err != nil {
            log.Error("msg", "Error on closing values statement", "error", err)
            return 0, err
        }
    }

    err = copyStmt.Close()
    if err != nil {
        log.Error("msg", "Error on COPY Close when writing samples", "error", err)
        return 0, err
    }

    err = tx.Commit()
    if err != nil {
        log.Error("msg", "Error on Commit when writing samples", "error", err)
        return 0, err
    }
    return sentCount, nil
}

func (c *Client) Write(ctx context.Context, id int, attempt int, metrics []string, count int) error {
    receivedMetrics.Add(float64(count))

    log.Debug("worker", id, "msg", "Start shipping metrics", "metrics", count, "attempt", attempt)

    begin := time.Now()
    sentCount, err := c.Insert(ctx, metrics)
    duration := time.Since(begin).Seconds()

    if err != nil {
        failedMetrics.WithLabelValues(c.Name()).Add(float64(count))
        return err
    }

    log.Debug("worker", id, "msg", "End shipping metrics", "metrics", sentCount, "attempt", attempt, "duration", duration)

    sentMetrics.WithLabelValues(c.Name()).Add(float64(sentCount))
    sentDuration.WithLabelValues(c.Name()).Observe(duration)

    return nil
}

func (c *Client) Close() {
    log.Info("msg", "Closing DB connection")
    if c.DB != nil {
        if err := c.DB.Close(); err != nil {
            log.Error("msg", "Error closing DB connection", "error", err.Error())
        }
    }
}

func (c *Client) Name() string {
    return "kafka-timescaledb-adapter"
}
