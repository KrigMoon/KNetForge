package Journal

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/KrigMoon/KNetForge/Help"
	pb "github.com/KrigMoon/KNetForge/Proto/gen"
)

//region Настройки

type SaveToDirData struct {
	ID         string
	DIR        string
	MODE       pb.ContainerMode
	LOGRULES   func(*pb.LogRecord) bool
	EVENTRULES func(*pb.EventRecord) bool
}

type JournalSettings struct {
	SaveToDir *SaveToDirData
}

//endregion

//region Структуры

type Journal struct {
	//* Статус
	status pb.Status
	//* Жизненный цикл
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
	readers sync.Map
	//* Настройки
	settings *JournalSettings
}

//endregion

//region Запуск

func (j *Journal) Run(settings *JournalSettings) {
	if j.status != pb.Status_OFF {
		return
	}

	defer func() { go j.stop() }()

	//* Инициализация
	j.ctx, j.cancel = context.WithCancel(context.Background())
	j.stopOnce.Reset()
	j.stopAwait = make(chan struct{})
	j.settings = settings

	//* Включение
	j.status = pb.Status_RUNNING

	if settings != nil {
		j.startSaveToDir()
	}

	j.status = pb.Status_ON
}

//endregion

//region Остановка

func (j *Journal) EndWork() {
	if j.status != pb.Status_ON {
		return
	}
	j.stopOnce.Do(func() {
		j.cancel()
		<-j.stopAwait
	})
}

func (j *Journal) stop() {
	<-j.ctx.Done()

	//* Выключение
	j.status = pb.Status_STOP
	j.writersWait.Wait()
	j.readersWait.Wait()
	j.status = pb.Status_OFF

	close(j.stopAwait)
}

//endregion
