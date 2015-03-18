package main

// Inspired by
// https://github.com/go-sql-driver/mysql/blob/f4bf8e8e0aa93d4ead0c6473503ca2f5d5eb65a8/utils.go#L34

// parseDSN parses the DSN string to a config
import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var (
	dsnPattern *regexp.Regexp // Data Source Name Parser
)

func init() {
	dsnPattern = regexp.MustCompile(
		`^(?:(?P<user>.*?)(?::(?P<passwd>.*))?@)?` + // [user[:password]@]
			`(?:(?P<net>[^\(]*)(?:\((?P<addr>[^\)]*)\))?)?` + // [net[(addr)]]
			`\/(?P<dbname>.*?)` + // /dbname
			`(?:\?(?P<params>[^\?]*))?$`) // [?param1=value1&paramN=valueN]

}

type config struct {
	user    string
	passwd  string
	net     string
	addr    string
	dbname  string
	params  map[string]string
	loc     *time.Location
	timeout time.Duration
}

func parseDSN(dsn string) (cfg *config, err error) {
	cfg = &config{
		net:  "tcp",
		addr: "localhost:8081",
		loc:  time.UTC,
		params: map[string]string{
			"s": fmt.Sprintf("session-1"),
		},
	}

	matches := dsnPattern.FindStringSubmatch(dsn)
	names := dsnPattern.SubexpNames()

	for i, match := range matches {
		switch names[i] {
		case "user":
			cfg.user = match
		case "passwd":
			cfg.passwd = match
		case "net":
			cfg.net = match
		case "addr":
			cfg.addr = match
		case "dbname":
			cfg.dbname = match
		case "params":
			for _, v := range strings.Split(match, "&") {
				param := strings.SplitN(v, "=", 2)
				if len(param) != 2 {
					continue
				}

				// cfg params
				switch value := param[1]; param[0] {

				// Time Location
				case "loc":
					cfg.loc, err = time.LoadLocation(value)
					if err != nil {
						return
					}

				// Dial Timeout
				case "timeout":
					cfg.timeout, err = time.ParseDuration(value)
					if err != nil {
						return
					}

				default:
					cfg.params[param[0]] = value
				}
			}
		}
	}

	return
}

// Returns the bool value of the input.
// The 2nd return value indicates if the input was a valid bool value
func readBool(input string) (value bool, valid bool) {
	switch input {
	case "1", "true", "TRUE", "True":
		return true, true
	case "0", "false", "FALSE", "False":
		return false, true
	}

	// Not a valid bool value
	return
}
