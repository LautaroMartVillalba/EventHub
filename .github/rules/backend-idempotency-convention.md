---
topic: backend-idempotency-convention
expert: Exp-Backend
date: 2026-07-29
scope: backend
source: decision
status: active
---

## Regla 1: Idempotency key extraction priority (Header > Body > Generate)
**Contexto**: El sistema debe soportar idempotencia para evitar eventos duplicados. El cliente puede proveer la key explícitamente o el sistema debe generarla.
**Decisión**: Usar el middleware `idempotency.Middleware()` que extrae la key con esta prioridad:
1. Header HTTP `Idempotency-Key` (máxima prioridad — control explícito del cliente)
2. Campo JSON `idempotency_key` en el body de la request
3. Generación determinística vía `GenerateKey(type, payload) = SHA256(type + ":" + payload)[:16]` en hex (32 chars)
**Motivo**: Header tiene prioridad porque el cliente puede querer anular la key del body. La generación automática garantiza idempotencia incluso cuando el cliente no envía key. SHA256 truncado a 16 bytes es suficiente para este dominio.
**Ámbito**: Todos los handlers HTTP que inserten eventos.
**Alternativas**: Solo header (no cubre clientes sin key), solo body (header es más semántico para metadata HTTP).
**Ejemplo**: Ver `internal/idempotency/middleware.go`

## Regla 2: Body size limit (1 MiB) con 413 en middleware
**Contexto**: El middleware lee el body para extraer idempotency_key/type/payload. Un body gigante puede agotar memoria (DoS).
**Decisión**: Usar `io.LimitReader(r.Body, 1<<20 + 1)` (1 MiB + 1 byte) antes de `io.ReadAll`. Si el body excede 1 MiB, retornar HTTP 413 y loguear WARN.
**Motivo**: Protección anti-DoS sin depender de autenticación. 1 MiB es suficiente para payloads de eventos típicos.
**Ámbito**: Middleware en `internal/idempotency/middleware.go`
**Alternativas**: `http.MaxBytesReader` (similar, requiere la ResponseWriter que ya tenemos).
**Ejemplo**: `limitedBody := io.LimitReader(r.Body, maxBodySize+1).(*io.LimitedReader)`

## Regla 3: Context key con tipo privado para evitar colisiones
**Contexto**: Se almacena idempotency_key en context.Context. Si la key es un string simple, otro paquete podría sobrescribirla accidentalmente.
**Decisión**: Usar `type contextKey string` privado (no exportado) como tipo de la key. La constante `ctxKey` también es privada.
**Motivo**: Go recomienda tipos personalizados para context keys. Previene colisiones con valores de contexto de otros paquetes.
**Ámbito**: Paquetes que usen context.Context para transportar valores (idempotency, logging, etc.)
**Alternativas**: Usar `int` como key (menos legible), usar string pública (riesgo de colisión).
**Ejemplo**: `type contextKey string; const ctxKey contextKey = "idempotency_key"`
