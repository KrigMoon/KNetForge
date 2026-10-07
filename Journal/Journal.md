# Описание

`Journal` — логирование и события. Через него любая часть системы может писать и читать логи и события. Логи и события — два независимых потока со своими читателями и каналами.

---

# Жизненный цикл

- `Run(js *JournalSettings)` — запуск. `js` может быть `nil` — тогда настройки идут по умолчанию.
- `EndWork()` — остановка.

Остановка ждёт всех писателей и читателей. Если кто-то не снял себя — остановка будет ждать вечность.

---

# Сущности

- **Читатель** — подписчик, получает `id`, у каждого свой канал. Режим чтения один: или `Logs`, или `Event`.
- **Писатель** — пишет логи и события, учитывается счётчиком.
- **Лог** (`LogRecord`):
    - `level` — Level(`INFO`, `WARNING`, `ERROR`);
    - `code` — LogCode(`LifeCircle`, `Net`, `CMD`, `Data`);
    - `time` — время создания;
    - `desc` — описание.
- **Событие** (`EventRecord`):
    - `level` — Level(`INFO`, `WARNING`, `ERROR`);
    - `code` — EventCode(`PeerConnection`);
    - `time` — время создания;
    - `metadata` — метаданные.
- **Режим** (`ContainerMode`) — `Logs` или `Event`. Определяет, в какой поток регистрируется читатель.

---

# Взаимодействие

> **Внимание**
> 1. Снять себя обязательно — иначе `EndWork` будет ждать вечность.
> 2. Читатель видит только то, что пришло после его открытия. Прошлые записи не восстанавливаются.

## Писатели

### AddWriter

`AddWriter()` — регистрирует писателя.

Писатель учитывается счётчиком. `EndWork` ждёт, пока все писатели снимут себя.

### WriteLog / WriteEvent

`WriteLog(log)` — рассылает лог всем читателям режима `Logs`.

`WriteEvent(event)` — рассылает событие всем читателям режима `Event`.

`WriteLog` блокирующий: ждёт, пока каждый читатель примет запись. `WriteEvent` не блокирует: при переполнении канала вытесняет из него самую старую запись и кладёт новую.

### DelWriter

`DelWriter()` — снимает писателя.

Счётчик уменьшается, `EndWork` перестаёт ждать этого писателя.

## Читатели

### AddReader

`AddReader(id *string, mode)` — регистрирует читателя, возвращает его `id`.

- `id == nil` — `id` генерируется автоматически.
- `id != nil` — используется переданное значение.
- Если читатель с таким `id` уже зарегистрирован — повторная регистрация не создаёт новый канал и не увеличивает счётчики.

`mode` — `Logs` или `Event`. Режим фиксируется при регистрации.

### ReadLog / ReadEvent

`ReadLog(id)` — читает лог из канала читателя `id`.

`ReadEvent(id)` — читает событие из канала читателя `id`.

Обе неблокирующие: если в канале пусто, возвращают `nil, false`.

### DelReader

`DelReader(id, mode)` — снимает читателя.

`mode` должен совпадать с тем, что был при `AddReader`. Если читателя с таким `id` нет — вызов игнорируется.

## Фабрики

### NewLog / NewEvent

`NewLog(level, code, desc)` — создаёт лог.

`NewEvent(level, code, metadata)` — создаёт событие.

---

# Сбор данных

`GetMetrics()` возвращает `JournalMetrics`:
- статус;
- число писателей;
- число читателей;
- заполнение каналов каждого читателя — отдельно для логов и отдельно для событий.

`ToFormatedText()` — то же самое готовым текстом.

---
# Настройки

Передаются в `Run`. Может быть `nil` — тогда применяются настройки по умолчанию.
```go
type JournalSettings struct {
    SaveLogsToDir   *SaveToDir   // nil — логи не сохраняются
    SaveEventsToDir *SaveToDir   // nil — события не сохраняются
}
```
## Автосохранение
```go
type SaveToDir struct {
    ID  string   // id внутреннего читателя (виден в метриках)
    DIR string   // папка для сохранения (создаётся, если её нет)
}
```
`SaveToDir` включает автосохранение одного потока (логов или событий) в папку.

1. `Journal` регистрирует внутреннего читателя с `ID` в нужном режиме.
2. Горутина читает из его канала и копит записи в буфер.
3. Буфер сбрасывается в файл при заполнении (512 записей) или при остановке.
4. Каждый сброс — новый файл в `DIR`. Имя — время сброса `2006-01-02_15-04-05`, расширение `.logs` или `.events`.

Файловые горутины слушают собственный контекст, отменяемый после снятия всех писателей.
# Пример

```go
// Настройки автосохранения
js := &Journal.JournalSettings{
    SaveLogsToDir:   &Journal.SaveToDir{ID: "lroot", DIR: "./records/logs"},
    SaveEventsToDir: &Journal.SaveToDir{ID: "eroot", DIR: "./records/events"},
}

// Запуск
journal := Journal.Journal{}
journal.Run(js)

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

var wg sync.WaitGroup
wg.Add(4)

// Читатель логов
go func() {
    defer wg.Done()
    logID := "logs"
    logID = journal.AddReader(&logID, pb.ContainerMode_Logs)
    defer journal.DelReader(logID, pb.ContainerMode_Logs)
    for {
        select {
        case <-ctx.Done():
            return
        default:
        }
        log, ok := journal.ReadLog(logID)
        if ok {
            fmt.Printf("Log:   [%v] (%s:%s) %s\n",
                log.Time.AsTime().Format("15:04:05"),
                log.Level, log.Code, log.Desc)
            continue
        }
        select {
        case <-ctx.Done():
            return
        case <-time.After(10 * time.Millisecond):
        }
    }
}()

// Читатель событий
go func() {
    defer wg.Done()
    eventID := "events"
    eventID = journal.AddReader(&eventID, pb.ContainerMode_Event)
    defer journal.DelReader(eventID, pb.ContainerMode_Event)
    for {
        select {
        case <-ctx.Done():
            return
        default:
        }
        event, ok := journal.ReadEvent(eventID)
        if ok {
            fmt.Printf("Event: [%v] (%s:%s) %s\n",
                event.Time.AsTime().Format("15:04:05"),
                event.Level, event.Code, event.Metadata)
            continue
        }
        select {
        case <-ctx.Done():
            return
        case <-time.After(10 * time.Millisecond):
        }
    }
}()

// Писатель логов
go func() {
    defer wg.Done()
    journal.AddWriter()
    defer journal.DelWriter()
    ticker := time.NewTicker(250 * time.Millisecond)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            journal.WriteLog(Journal.NewLog(
                pb.Level_INFO,
                pb.LogCode_Data.String(),
                "Запись лога"))
        }
    }
}()

// Писатель событий
go func() {
    defer wg.Done()
    journal.AddWriter()
    defer journal.DelWriter()
    ticker := time.NewTicker(250 * time.Millisecond)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            journal.WriteEvent(Journal.NewEvent(
                pb.Level_WARNING,
                pb.EventCode_PeerConnection.String(),
                "Запись события"))
        }
    }
}()

// Время на работу журнала
time.Sleep(2 * time.Second)

// Остановка
cancel()
wg.Wait()
journal.EndWork()
```