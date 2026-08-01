---
topic: httpapi-conventions
expert: Exp-Backend
date: 2026-07-29
scope: backend
source: decision
status: active
---

## Regla 1: DTOs JSON separados del dominio
**Contexto**: Los tipos de domain (Event, Process) no tienen tags json; exponerlos directo al HTTP los serializaría con nombres de campo Go (exportados).
**Decisión**: La API define DTOs propios con json tags (snake_case) en internal/httpapi/response.go (eventResponse, processResponse, createEventResponse, errorResponse) y conversores (newEventResponse, newProcessResponse).
**Motivo**: Mantener domain limpio de detalles de transporte; controlar exactamente el contrato JSON expuesto.
**Ámbito**: internal/httpapi — cualquier nueva ruta/respuesta debe usar DTOs del paquete, nunca domain directo.
**Alternativas**: Poner tags json en domain (acopla domain al HTTP).
**Ejemplo**: eventResponse{ID string `json:"id"`, ... Processes []processResponse `json:"processes"`}.

## Regla 2: Errores genéricos al cliente, detalle al log
**Contexto**: BackendValidator halló que un 400 por JSON inválido exponía err.Error() del parser (BV-001, MEDIUM).
**Decisión**: Las respuestas de error siempre devuelven {"error": "mensaje genérico y claro"} sin detalles internos (nunca err.Error() del parser/DB). El detalle se loguea con logger.Debug ("validation failed", reason, request_id) o logger.Error (errores 500).
**Motivo**: No filtrar información interna (tipos de error, rutas de parser) al cliente.
**Ámbito**: Todos los handlers de internal/httpapi.
**Alternativas**: Devolver el detalle completo al cliente (inseguro).
**Ejemplo**: 400 body inválido → {"error":"invalid JSON body"} + log Debug con el *json.SyntaxError.

## Regla 3: Código muerto defensivo — json.Valid sobre json.RawMessage
**Contexto**: TestingBackend halló (H1, LOW) que json.Valid(request.Payload) es siempre true: el parser padre ya validó los bytes de un json.RawMessage.
**Decisión**: No usar json.Valid sobre json.RawMessage para "validar" el payload. Si se quiere exigir que payload sea un objeto JSON (no string/número), usar json.Unmarshal + type switch.
**Motivo**: Evitar código muerto que da falsa sensación de validación.
**Ámbito**: internal/httpapi y cualquier paquete que parsee JSON.
**Alternativas**: Mantener json.Valid como defensa (innecesaria).
**Ejemplo**: NO: if !json.Valid(req.Payload) {...}; SÍ: var obj map[string]any; if err := json.Unmarshal(req.Payload, &obj); err != nil {...}.

## Regla 4: Orden de middlewares HTTP (httpapi)
**Contexto**: El orden define qué contexto ven los middlewares internos y qué puede loguear el recovery ante panic.
**Decisión**: r.Use(recovery → requestID → withLogger → slogAccessLog). Recovery es el más externo (captura todo) y por eso loguea con server.logger directo + X-Request-ID del header (WithLogger corre después). El middleware de idempotencia se aplica SOLO a POST /events (router.With).
**Motivo**: Recovery externo captura panics de todos; requestID temprano permite correlacionar logs; access log interno captura el status final.
**Ámbito**: internal/httpapi/server.go.
**Alternativas**: Recovery interno (no capturaría panics de otros middlewares).
**Ejemplo**: Ver NewServer.

## Regla 5: ShutdownTimeout default 30s (alineado con tests)
**Contexto**: Durante T07 se detectó que config default SHUTDOWN_TIMEOUT=150s no coincidía con TestLoadDefaults de Fase 1 (espera 30s) → go test ./... fallaba.
**Decisión**: El default de SHUTDOWN_TIMEOUT es 30s (alineado con la spec T02 y los tests). Cualquier cambio de default DEBE actualizar los tests de config.
**Motivo**: Consistencia spec ↔ tests ↔ defaults.
**Ámbito**: internal/config/config.go + internal/config/config_test.go.
**Alternativas**: Cambiar el test a 150s (rompía la spec T02).
**Ejemplo**: config.Load() sin env → ShutdownTimeout == 30 * time.Second.
