package main

import (
	"encoding/json"

	"github.com/wricardo/s3_log_parser/lib"
)

type Printer interface {
	PrintLogEntry(s3_log_parser.LogEntry)
}

type JSONPrinter struct {
	SlicePrinter
}

func (j *JSONPrinter) GetJson() ([]byte, error) {
	return json.Marshal(j.les)
}

type SlicePrinter struct {
	les []s3_log_parser.LogEntry
}

func (s *SlicePrinter) PrintLogEntry(le s3_log_parser.LogEntry) {
	if s.les == nil {
		s.les = make([]s3_log_parser.LogEntry, 0)
	}
	s.les = append(s.les, le)
}

func (s *SlicePrinter) GetSlice() []s3_log_parser.LogEntry {
	return s.les
}

func (s *SlicePrinter) Reset() {
	s.les = make([]s3_log_parser.LogEntry, 0)
}
