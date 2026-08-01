---
topic: ratelimit-batcher-conventions
expert: Exp-Backend
date: 2026-08-01
scope: backend
source: decision
status: active
---

## Regla 1: Backpressure natural — Submit bloquea, nunca responde 429
**Contexto**: La spec T08 exige que el ingesta no rechace eventos cuando la cola está llena; el cliente debe frenar naturalmente.
**Decisión**: Batcher.Submit bloquea en el envío al channel bufferizado (`batcher.items <- item`) cuando está lleno. El handler POST /events nunca devuelve 429; el bloqueo ES el mecanismo de regulación.
**Motivo**: Evitar rechazos y pérdidas de eventos bajo sobrecarga; el cliente se desacelera solo vía backpressure TCP/HTTP.
**Ámbito**: internal/ratelimit/batcher.go + internal/httpapi/handlers.go (createEvent).
**Alternativas**: Responder 429 (rechaza eventos, rompe at-least-once), descartar eventos (pérdida).
**Ejemplo**: `batcher.items <- item` bajo submitMu; handler: `if err := server.batcher.Submit(r.Context(), *event); err != nil {...}`.

## Regla 2: Invariante submitMu — serializar "check closed + enqueue" contra "set closed + close channel"
**Contexto**: La race send-on-closed-channel es el riesgo central de un batcher con Shutdown. Un diseño previo (shutdownCh + inFlight + dequeue spin) introdujo una race de pérdida de eventos (R-1).
**Decisión**: Todo envío al channel y el cierre del channel ocurren bajo el MISMO mutex `submitMu`. Submit: Lock → check closed → send → unlock. Shutdown: set closed → Lock → close(items) → unlock. El consumer hace `for item := range items` hasta channel cerrado y vacío.
**Motivo**: No existe ventana entre "aceptar" y "encolar" un evento; nada aceptado antes de Shutdown se pierde. Elimina estructuralmente la race R-1 sin depender de atómicos ni timers.
**Ámbito**: internal/ratelimit/batcher.go (Submit, Shutdown).
**Alternativas**: shutdownCh + inFlight + spin dequeue (rechazado: race R-1), close sin mutex (send-on-closed-channel).
**Ejemplo**: Ver batcher.go líneas ~153-187 (Submit) y ~197-205 (Shutdown).

## Regla 3: El resultado se propaga al handler por channel bufferizado cap 1
**Contexto**: El 201 de POST /events debe esperar a que el evento se haya persistido (spec T08), pero el consumer no puede quedar bloqueado si el cliente se desconecta.
**Decisión**: Cada queueItem lleva un `result chan error` con buffer 1. El consumer envía el resultado (nil/ErrConflict/error DB) sin bloquearse aunque el caller ya no lea. Submit espera en select { ctx.Done() | result }.
**Motivo**: El buffer 1 desacopla el ciclo de vida del caller del del consumer; el evento aceptado SIEMPRE se persiste aunque el cliente desaparezca.
**Ámbito**: internal/ratelimit/batcher.go (queueItem, Submit).
**Alternativas**: Devolver el error por referencia compartida (race), channel sin buffer (consumer bloqueable).
**Ejemplo**: `item.result <- err` (send bufferizado) + `select { case <-ctx.Done(): ... case persistErr := <-item.result: ... }`.

## Regla 4: context.WithoutCancel para escrituras — gotcha modernc/sqlite
**Contexto**: Con modernc.org/sqlite, cancelar un ctx a mitad de una transacción deja el desenlace indeterminado (el insert puede haberse commiteado mientras tx.Commit reporta un error falso) → pérdida silenciosa de un evento aceptado.
**Decisión**: El consumer deriva su contexto de escritura con `context.WithoutCancel(lifecycleCtx)`. Cancelar el lifecycle context señala el inicio del shutdown pero NUNCA aborta un insert en vuelo; el consumer drena hasta que Shutdown cierra el channel.
**Motivo**: Garantizar at-least-once: todo evento aceptado se persiste o se reporta su error, nunca "commiteado pero reportado como fallido".
**Ámbito**: internal/ratelimit/batcher.go (run), cmd/server/main.go (batcherCtx independiente del signal context).
**Alternativas**: Pasar ctx cancelable directo a las transacciones (desenlace indeterminado en modernc/sqlite).
**Ejemplo**: `writeCtx := context.WithoutCancel(lifecycleCtx)`; `for item := range items { handleItem(writeCtx, item) }`.

## Regla 5: Recover en consumer (run) y por-item (handleItem)
**Contexto**: Auditoría T08 (H1/H2): si handleItem/persistEvent paniqueaba, el consumer moría sin waitGroup.Done() → Shutdown se colgaba en waitGroup.Wait() para siempre; el caller de Submit quedaba esperando.
**Decisión**: (a) run() tiene defer con recover ANTES de waitGroup.Done() (el WaitGroup siempre queda balanceado). (b) handleItem() tiene defer con recover por-item: loguea, envía `fmt.Errorf("internal panic: %v")` al item.result del caller y hace failed.Add(1) — un panic en un evento no interrumpe el procesamiento de los demás.
**Motivo**: Un panic de persistencia no puede colgar el shutdown ni dejar callers esperando para siempre.
**Ámbito**: internal/ratelimit/batcher.go (run, handleItem).
**Alternativas**: Dejar morir la goroutine (Shutdown colgado), panic propagado (mata el server).
**Ejemplo**: TestConsumerPanic_DoesNotHangShutdown verifica que Shutdown retorna y el caller recibe el error.

## Regla 6: persistEvent delega TODO en InsertEvent (transacción única); no llamar InsertProcesses
**Contexto**: Auditoría T08 (H3): InsertEvent YA inserta los procesos embebidos en event.Processes en su única transacción. Llamar además InsertProcesses causaba doble inserción latente (violación de UNIQUE en event_processes.id) y escritura parcial si la 2ª TX fallaba.
**Decisión**: persistEvent llama SOLO a repository.InsertEvent (evento + procesos embebidos, atómicos). InsertProcesses queda como API pública del storage (sin callers en producción, documentado) para agregar procesos a eventos existentes en el futuro. Un evento nuevo se persiste vía InsertEvent únicamente.
**Motivo**: Atomicidad real (evento + procesos o nada) y cero riesgo de doble inserción.
**Ámbito**: internal/ratelimit/batcher.go (persistEvent), internal/httpapi/handlers.go (comentario createEvent).
**Alternativas**: Mantener la llamada duplicada a InsertProcesses (hazard documentado pero latente — rechazado).
**Ejemplo**: TestSubmit_EventWithProcesses_PersistsOnce: evento con 1 proceso embebido → 1 fila evento + 1 fila proceso, sin violación de PK.

## Regla 7: Semántica de contadores — processed = dequeueado Y manejado (incluye panic), failed excluye ErrConflict
**Contexto**: Auditoría T08 (N1): si persistFunc paniqueaba, processed no se incrementaba (el Add estaba después de la llamada) pero failed sí → Pending() sobre-estimaba y los contadores mentían.
**Decisión**: `processed.Add(1)` es la PRIMERA sentencia de handleItem (el evento fue dequeueado y manejado, su outcome se reporta al caller). failed solo cuenta errores distintos de ErrConflict (el conflicto idempotente es un resultado esperado, no un fallo).
**Motivo**: Invariante submitted >= processed en todo instante; métricas observables coherentes para logs de shutdown.
**Ámbito**: internal/ratelimit/batcher.go (handleItem, Processed, Failed, Pending).
**Alternativas**: Contar solo éxitos en processed (Pending sobre-estima tras panics — rechazado).
**Ejemplo**: TestConsumerPanic_DoesNotHangShutdown asevera Processed()==1 Y Failed()==1 tras un panic.

## Regla 8: Seam de test persistFunc (campo no exportado)
**Contexto**: Para testear el path de panic del consumer sin mockear SQLite, el batcher necesitaba un punto de inyección.
**Decisión**: Batcher tiene un campo no exportado `persistFunc func(ctx, event) error` inicializado en New() con un wrapper de persistEvent; handleItem lo invoca. Los tests del MISMO paquete lo reemplazan para inyectar panics/errores. No cambia la API pública.
**Motivo**: Testeabilidad sin interfaces ni mocks de DB; el paquete de tests interno (package ratelimit) puede acceder al campo.
**Ámbito**: internal/ratelimit/batcher.go (New, handleItem) + batcher_test.go.
**Alternativas**: Interfaz EventPersister (refactor mayor), mock de DB (complejo).
**Ejemplo**: `batcher.persistFunc = func(ctx context.Context, event domain.Event) error { panic("boom") }`.
