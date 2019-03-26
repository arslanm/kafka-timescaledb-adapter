package main

import (
    "time"

    "github.com/arslanm/kafka-timescaledb-adapter/db"
    "github.com/arslanm/kafka-timescaledb-adapter/kafka"

    "github.com/arslanm/kafka-timescaledb-adapter/util"
)

type Config struct {
    listenAddr      string
    telemetryPath   string
    pgKafkaConfig   pgkafka.Config
    pgDBConfig      pgdb.Config
    logLevel        string
    batchSize       int
    writeTimeout    time.Duration
    writeRetry      int
    whitelistFile   string
}

var (
    DEFAULT_LISTEN_ADDR    = ":9528"
    DEFAULT_TELEMETRY_PATH = "/metrics"
    DEFAULT_LOG_LEVEL      = "info"
    DEFAULT_BATCH_SIZE     = 10000
    DEFAULT_WRITE_TIMEOUT  = "30s"
    DEFAULT_WRITE_RETRY    = 3

    DEFAULT_WHITELIST_FILE = "/etc/prometheus/kafka-timescaledb-adapter.whitelist.regex"
)
    
func GetConfig() *Config {
    cfg := &Config{}

    cfg.listenAddr = util.GetEnvWithDefault("LISTEN_ADDR", DEFAULT_LISTEN_ADDR)
    cfg.telemetryPath = util.GetEnvWithDefault("TELEMETRY_PATH", DEFAULT_TELEMETRY_PATH)
    cfg.batchSize = util.GetEnvWithDefaultInt("BATCH_SIZE", DEFAULT_BATCH_SIZE)
    cfg.logLevel = util.GetEnvWithDefault("LOG_LEVEL", DEFAULT_LOG_LEVEL)
    cfg.writeTimeout = util.GetEnvWithDefaultDuration("WRITE_TIMEOUT", DEFAULT_WRITE_TIMEOUT)
    cfg.writeRetry = util.GetEnvWithDefaultInt("WRITE_RETRY", DEFAULT_WRITE_RETRY)
    cfg.whitelistFile = util.GetEnvWithDefault("WHITELIST_FILE", DEFAULT_WHITELIST_FILE)
   
    pgkafka.GetConfig(&cfg.pgKafkaConfig)
    pgdb.GetConfig(&cfg.pgDBConfig)

    return cfg
}
