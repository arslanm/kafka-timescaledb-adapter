package pgkafka

import (
    "os"

    "github.com/confluentinc/confluent-kafka-go/kafka"

    "github.com/arslanm/kafka-timescaledb-adapter/log"
    "github.com/arslanm/kafka-timescaledb-adapter/util"
)

type Config struct {
    brokerList  string
    groupId     string
    topic       string
}

var (
    DEFAULT_KAFKA_BROKER_LIST = "localhost:9092"
    DEFAULT_KAFKA_TOPIC       = "metrics"
    DEFAULT_KAFKA_GROUP_ID    = ""
)

func GetConfig(cfg *Config) *Config {

    cfg.brokerList = util.GetEnvWithDefault("KAFKA_BROKER_LIST", DEFAULT_KAFKA_BROKER_LIST)
    cfg.topic = util.GetEnvWithDefault("KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC)
    cfg.groupId = util.GetEnvWithDefault("KAFKA_GROUP_ID", DEFAULT_KAFKA_GROUP_ID)

    return cfg
}

func NewConsumer(cfg *Config) *kafka.Consumer {
    c, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers"               : cfg.brokerList,
        "group.id"                        : cfg.groupId,
        "session.timeout.ms"              : 6000,
        "go.events.channel.enable"        : true,
        "go.application.rebalance.enable" : true,
        "enable.partition.eof"            : true,
        "enable.auto.commit"              : true,
        "auto.offset.reset"               : "earliest",
    })

    if err != nil {
        log.Error("msg", "Failed to create consumer", "error", err)
        os.Exit(1)
    }

    log.Info("msg", "Created Kafka consumer", "consumer", c)

    topics := []string{cfg.topic}
    c.SubscribeTopics(topics, nil)
    return c
}
