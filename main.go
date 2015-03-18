package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	influxClient "github.com/influxdb/influxdb/client"
)

func file(name string, create bool) (*os.File, error) {
	switch name {
	case "stdin":
		return os.Stdin, nil
	case "stdout":
		return os.Stdout, nil
	default:
		if create {
			return os.Create(name)
		}
		return os.Open(name)
	}
}

type mymap map[string]interface{}

func (m mymap) keys() []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func (m mymap) values() []interface{} {
	var values []interface{}
	for _, v := range m {
		values = append(values, v)
	}
	return values
}

func influxdb(influxdbHost, inputs, output string) error {
	files := strings.Split(inputs, ",")
	srcs := make([]io.Reader, len(files))
	for i, f := range files {
		in, err := file(f, false)
		if err != nil {
			return err
		}
		defer in.Close()
		srcs[i] = in
	}
	cfg, err := parseDSN(influxdbHost)
	if err != nil {
		return err
	}

	log.Printf("%#v", cfg)

	out, err := file(output, false)
	if err != nil {
		return err
	}

	c, err := influxClient.NewClient(&influxClient.ClientConfig{
		Host:     cfg.addr,
		Username: cfg.user,
		Password: cfg.passwd,
		Database: cfg.dbname,
		IsSecure: false,
		IsUDP:    false,
	})

	if err != nil {
		panic(err)
	}

	for _, r := range srcs {
		msg := mymap{}
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			b := scanner.Bytes()
			if err := json.Unmarshal(b, &msg); err != nil {
				return err
			}
			if ts, ok := msg["timestamp"]; ok {
				t, err := time.Parse(time.RFC3339Nano, ts.(string))
				if err == nil {
					msg["time"] = t
				}
			}

			series := &influxClient.Series{
				Name:    cfg.params["s"], // s = series
				Columns: msg.keys(),
				Points: [][]interface{}{
					msg.values(),
				},
			}
			if err := c.WriteSeries([]*influxClient.Series{series}); err != nil {
				log.Println(err)
				continue
			}

			// pipe through
			if _, err := out.Write(b); err != nil {
				return err
			}
			// newline
			if _, err := out.Write([]byte{'\n'}); err != nil {
				return err
			}
		}

		if err := scanner.Err(); err != nil {
			return err
		}
	}

	// connect to influxdb
	// for each line
	//    parse json
	//    send as influxdb data point
	//    profit
	return nil
}

func influxdbCmd() command {
	fs := flag.NewFlagSet("ki influxdb", flag.ExitOnError)
	inputs := fs.String("inputs", "stdin", "Input files (comma separated). ")
	output := fs.String("output", "stdout", "Output file")
	influxdbHost := fs.String("db", "", "url to the influxdb host <username>:<pw>@tcp(<HOST>:<port>)/<dbname>")

	return command{fs, func(args []string) error {
		fs.Parse(args)
		return influxdb(*influxdbHost, *inputs, *output)
	}}
}

type command struct {
	fs *flag.FlagSet
	fn func(args []string) error
}

const Version = "0.1-alpha"

func main() {
	commands := map[string]command{
		"influxdb": influxdbCmd(),
	}

	fs := flag.NewFlagSet("ki", flag.ExitOnError)
	cpus := fs.Int("cpus", runtime.NumCPU(), "Number of CPUs to use")
	version := fs.Bool("version", false, "Print version and exit")

	fs.Usage = func() {
		fmt.Println("Usage: ki [global flags] <command> [command flags]")
		fmt.Printf("\nglobal flags:\n")
		fs.PrintDefaults()
		for name, cmd := range commands {
			fmt.Printf("\n%s command:\n", name)
			cmd.fs.PrintDefaults()
		}
	}

	fs.Parse(os.Args[1:])

	if *version {
		fmt.Println(Version)
		return
	}

	runtime.GOMAXPROCS(*cpus)

	args := fs.Args()
	if len(args) == 0 {
		fs.Usage()
		os.Exit(1)
	}

	if cmd, ok := commands[args[0]]; !ok {
		log.Fatalf("Unknown command: %s", args[0])
	} else if err := cmd.fn(args[1:]); err != nil {
		log.Fatal(err)
	}

}
