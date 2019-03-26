package util

import (
    "os"
    "time"
    "strconv"

    "github.com/arslanm/kafka-timescaledb-adapter/log"
)

func GetEnvWithDefault(env string, def string) string {
    tmp := os.Getenv(env)

    if tmp == "" {
        return def
    }
    return tmp
}

func GetEnvWithDefaultInt(env string, def int) int {
    tmp := os.Getenv(env)

    if tmp == "" {
        return def
    }

    i, err := strconv.Atoi(tmp)
    if err != nil {
        log.Error("error", err)
    }
    return i
}

func GetEnvWithDefaultDuration(env string, def string) time.Duration {
    tmp := os.Getenv(env)

    if tmp == "" {
        tmp = def
    }

    d, err := time.ParseDuration(tmp)

    if err != nil {
        log.Error("error", err)
    }
    return d
}

func GetEnvWithDefaultBool(env string, def bool) bool {
    tmp := os.Getenv(env)

    if tmp == "" {
        return def
    }

    b, err := strconv.ParseBool(tmp)
    if err != nil {
        log.Error("error", err)
    }
    return b
}
