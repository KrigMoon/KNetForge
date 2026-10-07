package Journal

import (
	"KNetForge/Help"
	pb "KNetForge/Proto/gen"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

//region Метрики

type JournalMetrics struct {
	Status                   pb.Status
	WritersCount             int
	ReadersCount             int
	ReadersLogsChanMetrics   map[string]Help.ChanMetrics
	ReadersEventsChanMetrics map[string]Help.ChanMetrics
}

func (jm *JournalMetrics) ToFormatedText() string {
	text := "Journal\n"
	text += fmt.Sprintf("    Статус: %s\n    Писатели: %d\n    Читатели: %d\n", jm.Status, jm.WritersCount, jm.ReadersCount)
	text += "    Читатели логов\n"
	for id, metrics := range jm.ReadersLogsChanMetrics {
		var idclear string
		if len(id) >= 8 {
			idclear = id[:8] + "..."
		} else {
			idclear = id
		}
		text += fmt.Sprintf("        %s: %d/%d\n", idclear, metrics.Len, metrics.Cap)
	}
	text += "    Читатели событий\n"
	for id, metrics := range jm.ReadersEventsChanMetrics {
		var idclear string
		if len(id) >= 8 {
			idclear = id[:8] + "..."
		} else {
			idclear = id
		}
		text += fmt.Sprintf("        %s: %d/%d\n", idclear, metrics.Len, metrics.Cap)
	}

	return text
}

func (jm *JournalMetrics) ToJSONString() string {
	if jm == nil {
		return "null"
	}

	out := struct {
		Status                   string                      `json:"status"`
		WritersCount             int                         `json:"writers_count"`
		ReadersCount             int                         `json:"readers_count"`
		ReadersLogsChanMetrics   map[string]Help.ChanMetrics `json:"readers_logs_chan_metrics"`
		ReadersEventsChanMetrics map[string]Help.ChanMetrics `json:"readers_events_chan_metrics"`
	}{
		Status:                   jm.Status.String(),
		WritersCount:             jm.WritersCount,
		ReadersCount:             jm.ReadersCount,
		ReadersLogsChanMetrics:   jm.ReadersLogsChanMetrics,
		ReadersEventsChanMetrics: jm.ReadersEventsChanMetrics,
	}

	data, err := json.Marshal(out)
	if err != nil {
		return `{"error":"` + err.Error() + `"}`
	}
	return string(data)
}

func (journal *Journal) GetMetrics() JournalMetrics {
	m := JournalMetrics{
		Status:                   journal.status,
		WritersCount:             int(journal.writersCount.Load()),
		ReadersCount:             int(journal.readersCount.Load()),
		ReadersLogsChanMetrics:   make(map[string]Help.ChanMetrics),
		ReadersEventsChanMetrics: make(map[string]Help.ChanMetrics),
	}

	journal.readersLogs.Range(func(k, v any) bool {
		ch := v.(chan *pb.LogRecord)
		m.ReadersLogsChanMetrics[k.(string)] = Help.ChanMetrics{
			Len: len(ch),
			Cap: cap(ch),
		}
		return true
	})

	journal.readersEvents.Range(func(k, v any) bool {
		ch := v.(chan *pb.EventRecord)
		m.ReadersEventsChanMetrics[k.(string)] = Help.ChanMetrics{
			Len: len(ch),
			Cap: cap(ch),
		}
		return true
	})

	return m
}

//endregion

//region Фабрики

func NewLog(level pb.Level, code string, desc string) *pb.LogRecord {
	return &pb.LogRecord{
		Level: level,
		Code:  code,
		Time:  timestamppb.Now(),
		Desc:  desc,
	}
}

func NewEvent(level pb.Level, code string, metadata string) *pb.EventRecord {
	return &pb.EventRecord{
		Level:    level,
		Code:     code,
		Time:     timestamppb.Now(),
		Metadata: metadata,
	}
}

//endregion

//region Сервисы

func (journal *Journal) createLogsToDir() {
	cfg := journal.js.SaveLogsToDir
	if cfg == nil {
		return
	}
	if _, err := os.Stat(cfg.DIR); os.IsNotExist(err) {
		if err := os.MkdirAll(cfg.DIR, 0o755); err != nil {
			return
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-journal.ctx.Done(); journal.writersWait.Wait(); cancel() }()

	id := cfg.ID
	journal.AddReader(&id, pb.ContainerMode_Logs)

	go journal.saveLogsToFile(ctx)
}

func (journal *Journal) saveLogsToFile(ctx context.Context) {
	cfg := journal.js.SaveLogsToDir
	defer journal.DelReader(cfg.ID, pb.ContainerMode_Logs)

	buf := make([]*pb.LogRecord, 0, 512)

	flush := func() {
		if len(buf) == 0 {
			return
		}
		name := time.Now().Format("2006-01-02_15-04-05") + ".logs"
		f, err := os.Create(filepath.Join(cfg.DIR, name))
		if err != nil {
			buf = buf[:0]
			return
		}
		for _, l := range buf {
			fmt.Fprintf(f, "[%s] (%s:%s) %s\n", l.Time.AsTime().Format("15:04:05"), l.Level, l.Code, l.Desc)
		}
		f.Close()
		buf = buf[:0]
	}

	for {
		if l, ok := journal.ReadLog(cfg.ID); ok {
			buf = append(buf, l)
			if len(buf) == cap(buf) {
				flush()
			}
			continue
		}
		select {
		case <-ctx.Done():
			flush()
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (journal *Journal) createEventsToDir() {
	cfg := journal.js.SaveEventsToDir
	if cfg == nil {
		return
	}
	if _, err := os.Stat(cfg.DIR); os.IsNotExist(err) {
		if err := os.MkdirAll(cfg.DIR, 0o755); err != nil {
			return
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-journal.ctx.Done(); journal.writersWait.Wait(); cancel() }()

	id := cfg.ID
	journal.AddReader(&id, pb.ContainerMode_Event)

	go journal.saveEventsToFile(ctx)
}

func (journal *Journal) saveEventsToFile(ctx context.Context) {
	cfg := journal.js.SaveEventsToDir
	defer journal.DelReader(cfg.ID, pb.ContainerMode_Event)

	buf := make([]*pb.EventRecord, 0, 512)

	flush := func() {
		if len(buf) == 0 {
			return
		}
		name := time.Now().Format("2006-01-02_15-04-05") + ".events"
		f, err := os.Create(filepath.Join(cfg.DIR, name))
		if err != nil {
			buf = buf[:0]
			return
		}
		for _, e := range buf {
			fmt.Fprintf(f, "[%s] (%s:%s) %s\n", e.Time.AsTime().Format("15:04:05"), e.Level, e.Code, e.Metadata)
		}
		f.Close()
		buf = buf[:0]
	}

	for {
		if e, ok := journal.ReadEvent(cfg.ID); ok {
			buf = append(buf, e)
			if len(buf) == cap(buf) {
				flush()
			}
			continue
		}
		select {
		case <-ctx.Done():
			flush()
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

//endregion
