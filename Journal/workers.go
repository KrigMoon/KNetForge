package Journal

import (
	pb "github.com/KrigMoon/KNetForge/Proto/gen"

	"github.com/google/uuid"
)

//region Читатели

func (journal *Journal) AddReader(id *string, mode pb.ContainerMode) string {
	var readerID string
	if id == nil {
		readerID = uuid.NewString()
	} else {
		readerID = *id
		switch mode {
		case pb.ContainerMode_Event:
			if _, ok := journal.readersEvents.Load(readerID); ok {
				return readerID
			}
		case pb.ContainerMode_Logs:
			if _, ok := journal.readersLogs.Load(readerID); ok {
				return readerID
			}
		}
	}

	journal.readersWait.Add(1)
	journal.readersCount.Add(1)

	switch mode {
	case pb.ContainerMode_Event:
		journal.readersEvents.Store(readerID, make(chan *pb.EventRecord, 512))
	case pb.ContainerMode_Logs:
		journal.readersLogs.Store(readerID, make(chan *pb.LogRecord, 512))
	}

	return readerID
}

func (journal *Journal) DelReader(id string, mode pb.ContainerMode) {
	var existed bool
	switch mode {
	case pb.ContainerMode_Event:
		_, existed = journal.readersEvents.LoadAndDelete(id)
	case pb.ContainerMode_Logs:
		_, existed = journal.readersLogs.LoadAndDelete(id)
	}

	if !existed {
		return
	}

	journal.readersCount.Add(-1)
	journal.readersWait.Done()
}

//endregion

//region Писатели

func (journal *Journal) AddWriter() {
	journal.writersWait.Add(1)
	journal.writersCount.Add(1)
}

func (journal *Journal) DelWriter() {
	journal.writersCount.Add(-1)
	journal.writersWait.Done()
}

//endregion
