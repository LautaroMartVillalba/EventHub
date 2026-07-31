---
topic: backend-repository-pattern
expert: Exp-Backend
date: 2026-07-29
scope: backend
source: decision
status: active
---

## Regla 1: Todas las escrituras en transacción
**Contexto**: El Repository debe garantizar consistencia entre events y event_processes.
**Decisión**: Usar helper `withTx(ctx, func(*sql.Tx) error)` para envolver toda operación de escritura.
**Motivo**: Asegura atomicidad entre escrituras múltiples (evento + processes). Rollback automático en error. Protección contra panic.
**Ámbito**: Todos los métodos de escritura en internal/storage/repository.go.
**Alternativas**: Transacciones manuales en cada método (duplicación).
**Ejemplo**: Ver `withTx` en repository.go

## Regla 2: Idempotency key con ON CONFLICT DO NOTHING
**Contexto**: El check de idempotency key con SELECT COUNT + INSERT tiene race condition (TOCTOU).
**Decisión**: Usar `INSERT ... ON CONFLICT(idempotency_key) DO NOTHING` + verificar `RowsAffected() == 0`.
**Motivo**: Operación atómica, la UNIQUE constraint garantiza consistencia bajo concurrencia.
**Ámbito**: InsertEvent en repository.go.
**Alternativas**: SELECT COUNT + INSERT (race condition), bloqueo pesimista (innecesario).
**Ejemplo**: `INSERT INTO events (...) VALUES (...) ON CONFLICT(idempotency_key) DO NOTHING`

## Regla 3: Procesos dead no se reintentan
**Contexto**: Status 'dead' en un proceso significa que agotó sus intentos y no debe reprocesarse.
**Decisión**: Filtrar con `status NOT IN ('completed', 'dead')` en lugar de `status != 'completed'`.
**Motivo**: Los procesos dead no deben ser recuperados ni reintentados.
**Ámbito**: FetchReadyEvents, FetchProcessesForRetry, RequeueEvent.

## Regla 4: Errores tipados con sentinel errors
**Contexto**: Separación de responsabilidades entre storage y capas superiores.
**Decisión**: Usar `var ErrNotFound = errors.New("record not found")` y `var ErrConflict = errors.New("idempotency key already exists")`.
**Motivo**: Las capas superiores pueden usar `errors.Is()` para distinguir errores sin depender de implementación SQL.
**Ámbito**: internal/storage/errors.go, usado en repository.go.
