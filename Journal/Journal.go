package Journal

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/KrigMoon/KNetForge/Help"
	pb "github.com/KrigMoon/KNetForge/Proto/gen"
)

//region Структура

type SaveToDir struct {
	ID  string
	DIR string
}

type JournalSettings struct {
	SaveLogsToDir   *SaveToDir
	SaveEventsToDir *SaveToDir
}

type Journal struct {
	//* Статус
	status pb.Status
	//* Жизненый цикл
	ctx       context.Context
	cancel    context.CancelFunc
	stopOnce  Help.DoOnce
	stopAwait chan struct{}
	//* Читатели и писатели (счётчики)
	readersWait  sync.WaitGroup
	readersCount atomic.Int32
	writersWait  sync.WaitGroup
	writersCount atomic.Int32
	//* Читатели (контейнеры)
	readersLogs     sync.Map
	readersEvents   sync.Map
	readersEventsMu sync.RWMutex
	//* Настройки
	js *JournalSettings
}

//endregion

//region Запуск

func (journal *Journal) Run(js *JournalSettings) {
	if journal.status != pb.Status_OFF {
		return
	}

	defer func() {
		go journal.stop()
	}()

	//* Инициализация
	journal.ctx, journal.cancel = context.WithCancel(context.Background())
	journal.stopOnce.Reset()
	journal.stopAwait = make(chan struct{})
	journal.js = js

	//* Включение
	journal.status = pb.Status_RUNNING

	if js != nil {
		journal.createLogsToDir()
		journal.createEventsToDir()
	}

	journal.status = pb.Status_ON
}

//endregion

//region Остановка

func (journal *Journal) EndWork() {
	if journal.status != pb.Status_ON {
		return
	}
	journal.stopOnce.Do(func() {
		journal.cancel()
		<-journal.stopAwait
	})
}

func (journal *Journal) stop() {
	<-journal.ctx.Done()

	//* Включение
	journal.status = pb.Status_STOP
	journal.writersWait.Wait()
	journal.readersWait.Wait()
	journal.status = pb.Status_OFF

	close(journal.stopAwait)
}

//endregion
