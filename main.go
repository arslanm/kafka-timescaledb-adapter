package main

import (
    "os"
    "fmt"
    "time"
    "os/signal"
    "syscall"
    "runtime"
    "net/http"

    "github.com/confluentinc/confluent-kafka-go/kafka"

    "github.com/arslanm/kafka-timescale-adapter/db"
    "github.com/arslanm/kafka-timescale-adapter/kafka"
    "github.com/arslanm/kafka-timescale-adapter/log"
    "github.com/arslanm/kafka-timescale-adapter/util"

    "github.com/prometheus/client_golang/prometheus"
)

func main() {
    cfg := GetConfig()

    numCPU := runtime.NumCPU()

    log.Init(cfg.logLevel)
    log.Debug("config", fmt.Sprintf("%+v",cfg))

    whiteList := util.LoadWhitelist(cfg.whitelistFile)

    db := pgdb.NewClient(&cfg.pgDBConfig, whiteList)
    defer db.Close()

    consumer := pgkafka.NewConsumer(&cfg.pgKafkaConfig)
    defer consumer.Close()

    Foreman(db.Write, cfg.writeTimeout, cfg.writeRetry, numCPU)

    http.Handle(cfg.telemetryPath, prometheus.Handler())
    go func() {
        err := http.ListenAndServe(cfg.listenAddr, nil)
        if err != nil {
            log.Error("msg", "Listen failure", "error", err)
            os.Exit(1)
        }
        log.Info("msg", fmt.Sprintf("Listening on %d for telemetry", cfg.listenAddr))
    }()

    sigchan := make(chan os.Signal, 1)
    signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

    req := WorkRequest{Metrics: make([]string, 0), NumMetrics: 0}

    run := true
    for run == true {
        select {
        case sig := <-sigchan:
            log.Info("msg", fmt.Sprintf("Received signal %v: terminating", sig))
            run = false
        case e := <- consumer.Events():
            switch ev := e.(type) {
            case kafka.AssignedPartitions:
                log.Info("msg", fmt.Sprintf("Assigning partition: %v", ev.Partitions))
                consumer.Assign(ev.Partitions)
            case kafka.RevokedPartitions:
                log.Info("msg", "Unassigning partition")
                consumer.Unassign()
            case *kafka.Message:
                req.Metrics = append(req.Metrics, string(ev.Value))
                req.NumMetrics += 1
                if req.NumMetrics == cfg.batchSize {
                    WorkQueue <- req
                    req = WorkRequest{Metrics: make([]string, 0), NumMetrics: 0}
                    <- CanSendMore
                }
            case kafka.PartitionEOF:
            case kafka.Error:
            }
        }
    }

    if req.NumMetrics > 0 {
        WorkQueue <- req
        time.Sleep(100 * time.Millisecond)
    }

    QuitChan <- true
    wg.Wait()
}
