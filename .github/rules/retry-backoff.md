---
topic: retry-backoff
expert: Exp-Backend
date: 2026-08-03
scope: backend
source: decision
status: active
---

## Regla 1: retry.Calculator usa attempts 1-based con schedule[min(attempts-1, len(schedule)-1)]
**Contexto**: La spec original de T13 proponía schedule[min(attempts, len(schedule)-1)], pero con attempts 1-based (el intento que acaba de fallar) eso desplazaba el schedule un lugar y rompía la semántica establecida en T11 (backoffFor: attempt 1 → schedule[0]) y sus tests.
**Decisión**: El paquete internal/retry interpreta attempts como 1-based: attempt 1 → schedule[0], attempt 2 → schedule[1], ... clamp a schedule[len-1] para attempts mayores. La fórmula interna es schedule[min(attempts-1, len(schedule)-1)]. isDead = attempts >= maxAttempts se mantiene exacto a la spec.
**Motivo**: Preservar la compatibilidad de comportamiento con T11 (fan-out) y que sus 18 tests existentes sigan pasando sin cambiar expectativas.
**Ámbito**: internal/retry y cualquier consumidor futuro del Calculator (fan-out, orchestrator).
**Alternativas**: Interpretar attempts como 0-based (rompía la coherencia con isDead y con los tests del fanout); aplicar la fórmula literal (off-by-one → tests fallaban).
**Ejemplo**: NewCalculator("2s,5s,15s", 3): ScheduleFor(1)=2s, ScheduleFor(2)=5s, ScheduleFor(3)=15s, ScheduleFor(4)=15s (clamp).

## Regla 2: El Calculator valida en el constructor con error; el FanOut recibe el Calculator ya construido
**Contexto**: Antes, NewFanOut(registry, repo, maxAttempts, backoffSchedule) panikeaba si maxAttempts < 1 y aceptaba schedule vacío con fallback inline (fallbackBackoff=1s).
**Decisión**: NewCalculator(backoffSchedule string, maxAttempts int) (*Calculator, error) retorna error si maxAttempts < 1 o si alguna parte del schedule no parsea como duración. Schedule vacío/whitespace → Calculator válido con fallback 1s (ScheduleFor/NextRetry). El FanOut recibe *retry.Calculator por inyección (no parsea config ni schedules) y solo panica por dependencias nil (registry, repo, calculator).
**Motivo**: Mover la validación de entrada al dominio (retry) en vez de a un panic en el worker; mantener la tolerancia a schedule vacío que ya tenía el fanout.
**Ámbito**: internal/retry, internal/workers/fanout.go.
**Alternativas**: Devolver Calculator con error sentinel por campo; panic en NewCalculator (descartado: el fanout no debe paniquear por config).
**Ejemplo**: retry.NewCalculator("2s,5s,15s,30s,60s", 5) → ok. retry.NewCalculator("2s,foo", 5) → error. retry.NewCalculator("", 5) → válido, ScheduleFor(1)=1s.

## Regla 3: Los tests de backoff viven en retry_test.go, no en fanout_test.go
**Contexto**: Al refactorizar T11, los tests TestBackoffFor_* y los asserts que usaban backoffFor directamente quedaron obsoletos.
**Decisión**: Los tests de indexación de schedule (posiciones, clamping, fallback, límites de isDead) viven en internal/retry/retry_test.go (11 tests, 100% cobertura). En fanout_test.go se usa el helper mustRetryCalculator(t, scheduleString, maxAttempts) para construir el Calculator y asserts vía calculator.ScheduleFor(...).
**Motivo**: La lógica de backoff es ahora del paquete retry; probarla en su propio paquete evita duplicación y acopla el test del fanout solo a su contrato público.
**Ámbito**: internal/retry/retry_test.go, internal/workers/fanout_test.go.
**Alternativas**: Mantener backoffFor como helper exportado del paquete workers (rechazado: duplicaba lógica).
**Ejemplo**: fanout_test.go: fanout := NewFanOut(reg, repo, mustRetryCalculator(t, "2s,5s,15s", 5)).
