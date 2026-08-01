---
topic: workers-orchestrator
expert: Exp-Backend
date: 2026-08-01
scope: backend
source: decision|convention|constraint
status: active
---

## Regla 1: El Orchestrator NUNCA cierra el canal ReadyEvents
**Contexto**: T09 implementó internal/workers/orchestrator.go; los consumidores del canal son el worker pool (T10/T11).
**Decisión**: Shutdown() cancela el contexto y espera a la goroutine de poll, pero jamás hace close() sobre ReadyEvents. El cierre del canal es responsabilidad exclusiva de los consumidores.
**Motivo**: Si el orchestrator cerrara el canal, los workers que lo drenan recibirían zero-value de domain.Event o panics al enviar, rompiendo su contrato de shutdown.
**Ámbito**: internal/workers — cualquier consumer de ReadyEvents (T10/T11) y el wiring de main.go (T18).
**Alternativas**: close() en Shutdown del orchestrator (descartado: rompe el contrato de los consumers).
**Ejemplo**: `func (o *Orchestrator) Shutdown() { o.shutdownOnce.Do(func() { if o.cancel != nil { o.cancel() }; o.waitGroup.Wait() }) }` — sin close.

## Regla 2: Handoff al canal es NO bloqueante; eventos dropeados no se pierden
**Contexto**: El poll envía a ReadyEvents con select de 3 vías (ch / ctx.Done / default).
**Decisión**: Si el canal está lleno, el evento se dropea para este tick (contador Dropped++). El evento NO se pierde: sigue status pending/partial_failed en la DB y el próximo tick lo vuelve a fetchear.
**Motivo**: Un worker pool lento no debe poder trabar el loop de poll (backpressure natural sin stall).
**Ámbito**: internal/workers orchestrator.go poll() y cualquier lógica que consuma el canal.
**Alternativas**: send bloqueante (descartado: puede detener el orchestrator), drop definitivo (descartado: pérdida de trabajo).
**Ejemplo**: `select { case o.ReadyEvents <- event: sent++; case <-ctx.Done(): return; default: dropped++ }`

## Regla 3: Start y Shutdown NO deben ejecutarse concurrentemente
**Contexto**: `cancel` se escribe dentro de startOnce (Start) y se lee dentro de shutdownOnce (Shutdown); no hay happens-before entre ambos sync.Once.
**Decisión**: La API declara en su doc comment que Start y Shutdown no son seguros para correr concurrentes (data race conocida en `cancel` y `waitGroup`, aceptada y documentada). Shutdown antes de Start es válido (no-op).
**Motivo**: Proteger ese escenario requiere atomic.Pointer o mutex con overhead innecesario para un uso que el contrato prohíbe.
**Ámbito**: internal/workers orchestrator.go; el wiring de T18 debe llamar Start una vez y Shutdown una vez, secuencialmente, durante el graceful shutdown.
**Alternativas**: atomic.Pointer[context.CancelFunc] (documentada como migración futura si T10+ requiere concurrencia).
**Ejemplo**: En main.go (T18): `orchestrator.Start(lifecycleCtx)` ... `orchestrator.Shutdown()` — nunca en paralelo.

## Regla 4: Logging del orchestrator — niveles y resolución por tick
**Contexto**: El contrato exige logging estructurado con slog y el logger del contexto (logging.FromContext).
**Decisión**: El logger se resuelve con logging.FromContext(ctx) en CADA tick (nunca cacheado en el struct). Nivel: Debug si fetched==0 (evitar ruido), Info con fetched_events/sent_events/dropped_events si fetched>0, Error con `error` si FetchReadyEvents falla (el loop continúa — nunca detener el orchestrator por un fetch fallido). Panic recovery loguea `panic` + `stack` (runtime/debug.Stack()).
**Motivo**: Consistencia con el estilo de la casa (ratelimit.Batcher) y observabilidad de producción sin flood de logs.
**Ámbito**: internal/workers y futuros paquetes de la pipeline.
**Alternativas**: logger cacheado en el struct (descartado: viola el contrato de FromContext).
**Ejemplo**: `logger.Debug("orchestrator poll found no ready events")` / `logger.Info("orchestrator forwarded ready events", "fetched_events", n, "sent_events", s, "dropped_events", d)` / `logger.Error("orchestrator failed to fetch ready events", "error", err)`

## Regla 5: Testabilidad — interfaz mínima EventFetcher como seam
**Contexto**: El Orchestrator depende de repository.FetchReadyEvents pero no debe acoplarse a *storage.Repository concreto.
**Decisión**: Se define una interfaz local mínima `EventFetcher { FetchReadyEvents(ctx context.Context, limit int) ([]domain.Event, error) }` que *storage.Repository satisface naturalmente; el constructor New la recibe. Tests inyectan fakes deterministas.
**Motivo**: Tests deterministas sin DB (24 tests, cobertura 98.1%) y desacoplamiento de la capa de storage.
**Ámbito**: internal/workers — patrón a replicar en T10/T11 para los consumers.
**Alternativas**: seam con campo func (estilo persistFunc del Batcher) — válido pero se eligió interfaz para el fetch.
**Ejemplo**: `fetcher := &fakeFetcher{events: []domain.Event{{ID: "ev-1"}}}; orch := workers.New(5*time.Millisecond, 5, 10, fetcher)`
