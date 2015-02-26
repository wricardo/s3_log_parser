package s3_log_parser

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
)

var REGEX string = "([^\\s\"'\\[\\]]+|\"([^\"]*)\"|\\[([^\\]']*)\\])"
var COMPILED_REGEX *regexp.Regexp

var fields_map map[int]string = map[int]string{
	0:  "Bucket Owner",
	1:  "Bucket",
	2:  "Time",
	3:  "Remote IP",
	4:  "Requester",
	5:  "Request ID",
	6:  "Operation",
	7:  "Key",
	8:  "Request-URI",
	9:  "HTTP status",
	10: "Error code",
	11: "Bytes Sent",
	12: "Object Size",
	13: "Total Time",
	14: "Turn-Around Time",
	15: "Referrer",
	16: "User-Agent",
	17: "Version Id",
}

type LogEntry map[string]string

func init() {
	COMPILED_REGEX, _ = regexp.Compile(REGEX)
}

type Parser struct {
	Concurrency int
}

func (p Parser) Parse(input io.Reader) ([]LogEntry, error) {
	if p.Concurrency == 0 {
		return nil, errors.New("Invalid Concurrency attr")
	}

	les := make([]LogEntry, 0)
	in := make(chan string, 0)
	out := make(chan LogEntry, 0)

	var wg sync.WaitGroup
	for x := 0; x < p.Concurrency; x++ {
		wg.Add(1)
		go parseLine(&wg, in, out)
	}

	scanner := bufio.NewScanner(input)
	go func() {
		for scanner.Scan() {
			in <- scanner.Text()
		}
		close(in)
		wg.Wait()
		close(out)
	}()

	for entry := range out {
		les = append(les, entry)
	}

	return les, scanner.Err()
}

func parseLine(wg *sync.WaitGroup, c chan string, out chan LogEntry) {
	var parts []string
	for line := range c {
		parts = COMPILED_REGEX.FindAllString(line, -1)
		le := make(LogEntry)
		for i, v := range parts {
			le[getFieldName(i)] = strings.Trim(v, "[]\"")
		}
		out <- le
	}
	wg.Done()
}

func getFieldName(i int) string {
	if val, ok := fields_map[i]; ok {
		return val
	} else {
		return "UNKNOWN_FIELD_" + fmt.Sprint(i)
	}
}
