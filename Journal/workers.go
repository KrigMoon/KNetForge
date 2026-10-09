package Journal

import (
	pb "github.com/KrigMoon/KNetForge/Proto/gen"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//region Контейнер

type reader struct {
	id         string
	mode       pb.ContainerMode
	logsRules  func(*pb.LogRecord) bool
	eventRules func(*pb.EventRecord) bool
	chanLogs   chan *pb.LogRecord
	chanEvents chan *pb.EventRecord
}

const readerChanBuf = 512

func (j *Journal) loadReader(id string) (reader, bool) {
	v, ok := j.readers.Load(id)
	if !ok {
		return reader{}, false
	}
	return v.(reader), true
}

//endregion

//region Сущности

//region^ Читатели

func (j *Journal) AddReader(
	id *string,
	mode pb.ContainerMode,
	logsRules func(*pb.LogRecord) bool,
	eventRules func(*pb.EventRecord) bool,
) string {
	readerID := uuid.NewString()
	if id != nil {
		readerID = *id
		if _, exists := j.readers.Load(readerID); exists {
			return readerID
		}
	}

	//* Дефолт — пропускать всё
	if logsRules == nil {
		logsRules = func(*pb.LogRecord) bool { return true }
	}
	if eventRules == nil {
		eventRules = func(*pb.EventRecord) bool { return true }
	}

	j.readersWait.Add(1)
	j.readersCount.Add(1)

	r := reader{
		id:         readerID,
		mode:       mode,
		logsRules:  logsRules,
		eventRules: eventRules,
	}
	if mode == pb.ContainerMode_Logs || mode == pb.ContainerMode_Both {
		r.chanLogs = make(chan *pb.LogRecord, readerChanBuf)
	}
	if mode == pb.ContainerMode_Event || mode == pb.ContainerMode_Both {
		r.chanEvents = make(chan *pb.EventRecord, readerChanBuf)
	}

	j.readers.Store(readerID, r)
	return readerID
}

func (j *Journal) DelReader(id string) {
	if _, existed := j.readers.LoadAndDelete(id); !existed {
		return
	}
	j.readersCount.Add(-1)
	j.readersWait.Done()
}

//endregion^

//region^ Писатели

func (j *Journal) AddWriter() {
	j.writersWait.Add(1)
	j.writersCount.Add(1)
}

func (j *Journal) DelWriter() {
	j.writersCount.Add(-1)
	j.writersWait.Done()
}

//endregion^

//endregion

//region Чтение/Запись

func (j *Journal) Read(id string) (*pb.Record, bool) {
	r, ok := j.loadReader(id)
	if !ok {
		return nil, false
	}

	//* Логи
	if r.chanLogs != nil {
		select {
		case log, ok := <-r.chanLogs:
			if ok {
				return &pb.Record{Log: log}, true
			}
		default:
		}
	}

	//* События
	if r.chanEvents != nil {
		select {
		case event, ok := <-r.chanEvents:
			if ok {
				return &pb.Record{Event: event}, true
			}
		default:
		}
	}

	return nil, false
}

func (j *Journal) Write(record *pb.Record) {
	j.readers.Range(func(_, v any) bool {
		r := v.(reader)

		//* Лог
		if record.Log != nil && r.chanLogs != nil && r.logsRules(record.Log) {
			r.chanLogs <- record.Log
		}

		//* Событие
		if record.Event != nil && r.chanEvents != nil && r.eventRules(record.Event) {
			select {
			case r.chanEvents <- record.Event:
			default:
				<-r.chanEvents
				r.chanEvents <- record.Event
			}
		}

		return true
	})
}

//endregion

//region Фабрики

func NewLog(level pb.Level, code, desc string) *pb.LogRecord {
	return &pb.LogRecord{
		Level: level,
		Code:  code,
		Time:  timestamppb.Now(),
		Desc:  desc,
	}
}

func NewEvent(level pb.Level, code, metadata string) *pb.EventRecord {
	return &pb.EventRecord{
		Level:    level,
		Code:     code,
		Time:     timestamppb.Now(),
		Metadata: metadata,
	}
}

//endregion
