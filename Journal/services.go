package Journal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/KrigMoon/KNetForge/Help"
	pb "github.com/KrigMoon/KNetForge/Proto/gen"
)

//region Метрики

type JournalMetrics struct {
	Status                   pb.Status
	WritersCount             int
	ReadersCount             int
	ReadersLogsChanMetrics   map[string]Help.ChanMetrics
	ReadersEventsChanMetrics map[string]Help.ChanMetrics
}

func (j *Journal) GetMetrics() JournalMetrics {
	m := JournalMetrics{
		Status:                   j.status,
		WritersCount:             int(j.writersCount.Load()),
		ReadersCount:             int(j.readersCount.Load()),
		ReadersLogsChanMetrics:   make(map[string]Help.ChanMetrics),
		ReadersEventsChanMetrics: make(map[string]Help.ChanMetrics),
	}

	j.readers.Range(func(k, v any) bool {
		r := v.(reader)
		id := k.(string)

		if r.chanLogs != nil {
			m.ReadersLogsChanMetrics[id] = Help.ChanMetrics{
				Len: len(r.chanLogs),
				Cap: cap(r.chanLogs),
			}
		}
		if r.chanEvents != nil {
			m.ReadersEventsChanMetrics[id] = Help.ChanMetrics{
				Len: len(r.chanEvents),
				Cap: cap(r.chanEvents),
			}
		}
		return true
	})

	return m
}

const shortIDLen = 8

func shortID(id string) string {
	if len(id) >= shortIDLen {
		return id[:shortIDLen] + "..."
	}
	return id
}

func (m *JournalMetrics) ToFormatedText() string {
	text := "Journal\n"
	text += fmt.Sprintf("    Статус: %s\n    Писатели: %d\n    Читатели: %d\n",
		m.Status, m.WritersCount, m.ReadersCount)

	text += "    Читатели логов\n"
	for id, cm := range m.ReadersLogsChanMetrics {
		text += fmt.Sprintf("        %s: %d/%d\n", shortID(id), cm.Len, cm.Cap)
	}

	text += "    Читатели событий\n"
	for id, cm := range m.ReadersEventsChanMetrics {
		text += fmt.Sprintf("        %s: %d/%d\n", shortID(id), cm.Len, cm.Cap)
	}

	return text
}

func (m *JournalMetrics) ToJSONString() string {
	if m == nil {
		return "null"
	}

	out := struct {
		Status                   string                      `json:"status"`
		WritersCount             int                         `json:"writers_count"`
		ReadersCount             int                         `json:"readers_count"`
		ReadersLogsChanMetrics   map[string]Help.ChanMetrics `json:"readers_logs_chan_metrics"`
		ReadersEventsChanMetrics map[string]Help.ChanMetrics `json:"readers_events_chan_metrics"`
	}{
		Status:                   m.Status.String(),
		WritersCount:             m.WritersCount,
		ReadersCount:             m.ReadersCount,
		ReadersLogsChanMetrics:   m.ReadersLogsChanMetrics,
		ReadersEventsChanMetrics: m.ReadersEventsChanMetrics,
	}

	data, err := json.Marshal(out)
	if err != nil {
		return `{"error":"` + err.Error() + `"}`
	}
	return string(data)
}

//endregion

//region Файловый сервис

func (j *Journal) startSaveToDir() {
	cfg := j.settings.SaveToDir
	if cfg == nil {
		return
	}
	if err := os.MkdirAll(cfg.DIR, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "journal: cannot create dir %q: %v\n", cfg.DIR, err)
		return
	}

	j.AddReader(&cfg.ID, cfg.MODE, cfg.LOGRULES, cfg.EVENTRULES)
	go j.saveToFile(cfg)
}

func (j *Journal) saveToFile(cfg *SaveToDirData) {
	defer j.DelReader(cfg.ID)

	var buf []*pb.Record

	flush := func() {
		if len(buf) == 0 {
			return
		}
		name := time.Now().Format("2006-01-02_15-04-05") + ".kj"
		f, err := os.Create(filepath.Join(cfg.DIR, name))
		if err == nil {
			for _, rec := range buf {
				if rec.Log != nil {
					fmt.Fprintf(f, "[%s] (%s:%s) %s\n",
						rec.Log.Time.AsTime().Format("15:04:05"),
						rec.Log.Level, rec.Log.Code, rec.Log.Desc)
				}
				if rec.Event != nil {
					fmt.Fprintf(f, "[%s] (%s:%s) %s\n",
						rec.Event.Time.AsTime().Format("15:04:05"),
						rec.Event.Level, rec.Event.Code, rec.Event.Metadata)
				}
			}
			f.Close()
		}
		buf = buf[:0]
	}

	for {
		if rec, ok := j.Read(cfg.ID); ok {
			buf = append(buf, rec)
			if len(buf) >= 512 {
				flush()
			}
			continue
		}

		flush()

		select {
		case <-j.ctx.Done():
			for {
				rec, ok := j.Read(cfg.ID)
				if !ok {
					break
				}
				buf = append(buf, rec)
			}
			flush()
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

//endregion
