package Journal

import (
	pb "KNetForge/Proto/gen"
)

//region Логи

func (journal *Journal) ReadLog(id string) (*pb.LogRecord, bool) {
	v, ok := journal.readersLogs.Load(id)
	if !ok {
		return nil, false
	}
	ch := v.(chan *pb.LogRecord)

	select {
	case log, ok := <-ch:
		return log, ok
	default:
		return nil, false
	}
}

func (journal *Journal) WriteLog(log *pb.LogRecord) {
	journal.readersLogs.Range(func(_, v any) bool {
		ch := v.(chan *pb.LogRecord)
		ch <- log
		return true
	})
}

//endregion

//region События

func (journal *Journal) ReadEvent(id string) (*pb.EventRecord, bool) {
	v, ok := journal.readersEvents.Load(id)
	if !ok {
		return nil, false
	}
	ch := v.(chan *pb.EventRecord)

	select {
	case event, ok := <-ch:
		return event, ok
	default:
		return nil, false
	}
}

func (journal *Journal) WriteEvent(log *pb.EventRecord) {
	journal.readersEventsMu.Lock()
	defer journal.readersEventsMu.Unlock()

	journal.readersEvents.Range(func(_, v any) bool {
		ch := v.(chan *pb.EventRecord)
		select {
		case ch <- log:
		default:
			<-ch
			ch <- log
		}
		return true
	})
}

//endregion
