package util

import (
    "os"
    "fmt"
    "bufio"
    "regexp"
    "strings"
    "github.com/arslanm/kafka-timescaledb-adapter/log"
)

type Whitelist struct {
    Items     []*regexp.Regexp
    NumItems  int
}

func LoadWhitelist(filename string) *Whitelist {
    file, err := os.Open(filename)
    if err != nil {
        log.Debug("msg", "Can't read whitelist file -- ignoring", "file", filename, "error", err)
        return nil
    }
    defer file.Close()

    items := make([]string, 0)
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        item := scanner.Text()
        item = strings.TrimSpace(item)
        if len(item) == 0 {
            continue
        }
        if fmt.Sprintf("%c", item[0]) == "#" {
            continue
        }
        items = append(items, item)
    }

    if err := scanner.Err(); err != nil {
        log.Debug("msg", "Can't read whitelist file", "file", filename, "error", err)
        return nil
    }

    itemLen := len(items)

    wl := &Whitelist{
        Items    : make([]*regexp.Regexp, 0, itemLen),
        NumItems : itemLen,
    }

    for i := 0; i < itemLen; i++ {
        re, err := regexp.Compile(items[i])
        if err != nil {
            log.Error("msg", "Can't compile regular expression", "expression", items[i], "error", err)
            os.Exit(1)
        }
        wl.Items = append(wl.Items, re)
    }
    log.Debug("msg", "Loaded whitelist", "count", wl.NumItems)
    return wl
}

func (wl Whitelist) IsWhitelisted(name string) bool {
    if &wl == nil {
        return true
    }

    whiteListed := false
    for i := 0 ; i < wl.NumItems ; i++ {
        if wl.Items[i].FindStringSubmatch(name) != nil {
            whiteListed = true
            break
        }
    }
    return whiteListed
}
