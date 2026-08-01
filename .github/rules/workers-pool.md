---
topic: workers-pool
expert: Exp-Backend
date: 2026-08-01
scope: backend
source: decision|convention|constraint
status: active
---

## Regla 1: El Pool cierra ReadyEvents; el orden de shutdown en el wiring es Orchestrator → Pool
**Contexto**: T10 implementó internal/workers/pool.go, el consumer del canal ReadyEvents del Orchestrator (T09).
**Decisión**: Pool.Shutdown() hace close(readyEvents) + waitGroup.Wait() (Regla 1 de workers-orchestrator: el consumer cierra el canal, el productor nunca). En el wiring T18, Orchestrator.Shutdown() DEBE ejecutarse ANTES que Pool.Shutdown(): el poll loop deja de enviar antes del close, así ningún send a canal cerrado puede ocurrir.
**Motivo**: send a canal cerrado = panic; el contrato del canal exige que el productor pare antes de que el consumidor cierre.
**Ámbito**: internal/workers pool.go; wiring de main.go (T18).
**Alternativas**: close desde el Orchestrator (descartado: rompe Regla 1 de T09); timeout en Pool.Shutdown (innecesario).
**Ejemplo**: `orchestrator.Shutdown(); pool.Shutdown()` — en ese orden, nunca invertido.

## Regla 2: Seam por interfaz mínima para el consumer (EventProcessor y EventStatusUpdater)
**Contexto**: El Pool depende de repository.UpdateEventStatus y de una función de procesamiento que T11 reemplazará por el fan-out.
**Decisión**: Dos interfaces locales mínimas: `EventProcessor { Process(ctx context.Context, event domain.Event) error }` (inyectable; T11 la implementa con fan-out sin tocar el pool) y `EventStatusUpdater { UpdateEventStatus(ctx context.Context, eventID string, status domain.EventStatus) error }` (*storage.Repository la satisface naturalmente). Los tests inyectan fakes deterministas.
**Motivo**: Testabilidad determinista sin DB y desacoplamiento de storage (misma filosofía que EventFetcher de T09).
**Ámbito**: internal/workers pool.go; T11 debe implementar EventProcessor sin tocar el Pool.
**Alternativas**: seam por campo func (estilo persistFunc del Batcher) — válido, pero se priorizó interfaz por simetría con T09.
**Ejemplo**: `pool := workers.NewPool(config.WorkersLevel2Count, orchestrator.ReadyEvents, &workers.LoggingProcessor{}, repository)`

## Regla 3: Semántica de contadores Processed()/Failed() — cuentan intentos, no eventos únicos
**Contexto**: Hallazgo F1 de la auditoría T10: pool.processed.Add(1) antes del update final producía doble conteo (processed y failed) del mismo evento si el update a completed paniqueaba.
**Decisión**: Processed() cuenta eventos cuyo Process() tuvo éxito Y cuyo estado completed se persistió (el Add va DESPUÉS del update exitoso). Failed() cuenta eventos cuyo Process falló/panicó o cuyo update a completed panicó. Un intento NUNCA cuenta el mismo evento como processed y failed a la vez. Bajo at-least-once el mismo evento puede contarse más de una vez a través de redeliveries.
**Motivo**: Invariante de consistencia contador↔estado persistido; los contadores miden intentos, no eventos únicos.
**Ámbito**: internal/workers pool.go (Processed/Failed y processEvent).
**Alternativas**: contadores de eventos únicos (requeriría dedup por ID, costo innecesario).
**Ejemplo**: update a completed con error → ni processed ni failed en ese intento; el evento queda stale (processing) y el poll lo redelivers.

## Regla 4: At-least-once — si falla el marcador processing, procesar igual
**Contexto**: Un worker que ya recibió el evento del canal no debe descartarlo porque el update a processing falló.
**Decisión**: Si UpdateEventStatus(processing) falla, loguear y CONTINUAR procesando; el update final (completed/partial_failed) corrige el registro. Si el update final falla, el evento queda stale en DB y un poll posterior lo redescubre.
**Motivo**: El evento ya fue entregado (handoff al canal); descartarlo perdería trabajo. El modelo es at-least-once con redelivery por poll.
**Ámbito**: internal/workers pool.go processEvent.
**Alternativas**: saltar el evento si processing falla (descartado: pérdida de trabajo en este ciclo).
**Ejemplo**: updater falla solo en processing → el evento se procesa igual y termina completed.

## Regla 5: Doble panic recovery en workers (por-item + por-goroutine antes de waitGroup.Done)
**Contexto**: Un panic en Process() o en un update de estado no debe matar el pool ni colgar el Shutdown.
**Decisión**: (a) recover por-item al inicio de processEvent (registrado ANTES de resolver el logger): evento → Failed()+1 + partial_failed best-effort; el worker continúa con el siguiente evento. (b) recover por-goroutine en runWorker ANTES de waitGroup.Done() (patrón idéntico al run del Orchestrator) para que un panic jamás deje el WaitGroup desbalanceado y cuelgue Shutdown. Ambos loguean panic + stack (runtime/debug.Stack()).
**Motivo**: Un pool debe ser resiliente a pánicos aislados sin degradar el shutdown.
**Ámbito**: internal/workers pool.go (runWorker, processEvent).
**Alternativas**: un solo recover por-goroutine (descartado: mataría al worker en el primer panic de un evento).
**Ejemplo**: processor paniquea → Failed()+1, partial_failed, el worker procesa el siguiente evento.

## Regla 6: El Pool no guarda cancel en el struct (sin data race Start/Shutdown)
**Contexto**: T09 documentó una data race aceptada en cancel entre Start y Shutdown (Regla 3 de workers-orchestrator). El Pool no debe replicarla.
**Decisión**: Pool.Start(ctx) pasa el ctx a las goroutines sin derivar contexto propio ni guardar cancel. Shutdown solo cierra el canal y espera. Si el parent se cancela, las llamadas con ctx abortan solas y sus errores se loguean.
**Motivo**: Sin campo mutable compartido entre Start y Shutdown, no hay carrera posible.
**Ámbito**: internal/workers pool.go.
**Alternativas**: atomic.Pointer[context.CancelFunc] (innecesario si no se cancela a mitad de proceso).
**Ejemplo**: `pool.Start(lifecycleCtx)` ... `pool.Shutdown()` — sin cancel intermedio.
