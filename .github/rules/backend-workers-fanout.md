---
topic: backend-workers-fanout
expert: Exp-Backend
date: 2026-08-01
scope: backend
source: decision
status: active
---

## Regla 1: Contrato EventProcessor↔Pool con sentinela ErrEventDead
**Contexto**: El Pool (T10) persiste el estado final del evento según el retorno de EventProcessor.Process (nil→completed, error→partial_failed). T11 (FanOut) introduce un tercer estado: dead vía MoveEventToDeadLetter. Si el procesador retorna error o nil tras dead-letterar, el pool pisaría 'dead' con partial_failed/completed, rompiendo el requeue API (solo acepta eventos 'dead') y el flujo de DLQ.
**Decisión**: Todo procesador que ya movió el evento a la dead-letter queue debe retornar el sentinela `ErrEventDead` (errors.New en workers). El pool detecta `errors.Is(err, ErrEventDead)` después de `failed.Add(1)` y ANTES de persistir partial_failed, y retorna sin pisar el estado 'dead'.
**Motivo**: At-least-once con 3 estados expresables sin cambiar la interfaz; retrocompatible (nil/error común conservan comportamiento previo).
**Ámbito**: internal/workers/pool.go, internal/workers/fanout.go, cualquier futuro EventProcessor.
**Alternativas**: Sentinela para los 3 estados (sobredimensionado), modificar la interfaz (rompe el seam).
**Ejemplo**: fanout.go case anyDead → MoveEventToDeadLetter + `return ErrEventDead`; pool.go `if errors.Is(err, ErrEventDead) { return }` antes de UpdateEventStatus(partial_failed).

## Regla 2: Política de backoff del FanOut
**Contexto**: La spec T11 requiere backoff configurable (config.BACKOFF_SCHEDULE, default "2s,5s,15s,30s,60s").
**Decisión**: El intento N (1-based, N = Attempts+1 tras el fallo) usa `schedule[min(N-1, len-1)]`. Tras un fallo: `newAttempts = Attempts+1`; si `newAttempts >= MAX_ATTEMPTS` → proceso dead (sin next_retry_at); si no → failed con `next_retry_at = now.UTC() + backoffFor(newAttempts)`. En éxito se persiste completed con `Attempts+1`. Schedule vacío → fallback 1s.
**Motivo**: Backoff exponencial-limitado predecible y testeable; el conteo de attempts es monotónico y sin doble conteo.
**Ámbito**: internal/workers/fanout.go (backoffFor, handleProcessFailure).
**Alternativas**: Exponential backoff con jitter (innecesario aquí), clamp a 0 (newAttempts siempre >= 1).

## Regla 3: Filtro ready/backoff en memoria del FanOut, no en la query
**Contexto**: FetchProcessesForRetry devuelve TODOS los procesos no-completados sin filtrar next_retry_at (método preexistente con tests). El Orchestrator entrega un evento cuando al menos UN proceso está listo, por lo que un evento multi-proceso puede contener procesos aún en backoff.
**Decisión**: El FanOut separa en memoria los procesos LISTOS (NextRetryAt == nil o <= now) de los EN BACKOFF (futuro); ejecuta solo los listos; cuenta los skipped; si skipped > 0 el evento NO se marca completed → retornar error → pool persiste partial_failed → el Orchestrator re-poll cuando venza el next_retry_at. Orden de agregación: anyDead → anyFailed → skipped>0 → nil.
**Motivo**: Si la query filtrara los procesos en backoff, el FanOut no sabría que quedaron procesos pendientes y un evento multi-proceso podría marcarse completed con procesos que nunca correrían.
**Ámbito**: internal/workers/fanout.go (Process).
**Alternativas**: Filtrar en la query (bug de caso multi-proceso), consulta adicional de totales (sobredimensionado).

## Regla 4: Dispatch registry — RWMutex, copias defensivas, Name == ProcessName
**Contexto**: El Registry (T12) se lee concurrentemente desde las goroutines del pool y se escribe en startup/T17.
**Decisión**: `Registry` con `map[string][]Process` protegido por `sync.RWMutex`; `Register` copia el slice de entrada; `GetProcesses` retorna copia defensiva (nil si el tipo no está registrado). `dispatch.Process.Name` debe coincidir con `domain.Process.ProcessName` (la fila de BD) para que el FanOut resuelva handler↔fila.
**Motivo**: Seguridad de concurrencia (reads paralelos, writes serializados) y protección contra mutación externa.
**Ámbito**: internal/dispatch/registry.go.
**Alternativas**: sync.Map (menos tipado), sin copias (mutable desde afuera).
**Ejemplo**: NewRegistry() → Register("user_created", []Process{{Name: "send_email", Fn: ...}}) → GetProcesses("user_created").

## Regla 5: Concurrencia del FanOut — WaitGroup + recover per-goroutine + resultados bajo mutex
**Contexto**: El FanOut ejecuta 1 goroutine por proceso y agrega resultados.
**Decisión**: `sync.WaitGroup` con Add antes de cada goroutine y Wait al final; recover per-goroutine (un panic de un process.Fn se trata como fallo del proceso, nunca deshace el FanOut ni fuga el WaitGroup); los resultados agregados (anyFailed/failedCount/anyDead/skipped) se escriben bajo `sync.Mutex` local y se leen tras Wait sin lock.
**Motivo**: Un panic aislado no debe matar el worker del pool ni colgar Shutdown (mismo patrón que pool.processEvent).
**Ámbito**: internal/workers/fanout.go.
**Alternativas**: errorgroup (dependencia externa), canal de resultados (más verboso).
