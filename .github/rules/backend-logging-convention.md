---
topic: backend-logging-convention
expert: Exp-Backend
date: 2026-07-29
scope: backend
source: decision
status: active
---

## Regla 1: JSON structured logs via slog con UTC/RFC3339Nano
**Contexto**: El proyecto necesita logs estructurados para ser consumidos por sistemas de monitoreo (Datadog, CloudWatch, etc.).
**Decisión**: Usar `slog.NewJSONHandler(os.Stdout, ...)` con `ReplaceAttr` que convierte timestamps `time.Time` a string UTC en formato `RFC3339Nano` (ej: "2026-07-29T15:04:05.123456789Z").
**Motivo**: JSON es parseable por cualquier sistema de logging. RFC3339Nano es el formato ISO 8601 estándar con precisión de nanosegundos. UTC evita ambigüedades de zona horaria.
**Ámbito**: Todo el logging del servidor. Configurado en `internal/logging/logging.go`.
**Alternativas**: Formato texto (no parseable), formato custom (no estándar).
**Ejemplo**: `slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level, ReplaceAttr: ...}))`

## Regla 2: Log level desde config vía ParseLevel
**Contexto**: El nivel de log debe ser configurable sin recompilar. La config ya tiene `LOG_LEVEL` como string.
**Decisión**: Usar `logging.ParseLevel(cfg.LogLevel)` que mapea strings case-insensitive: "debug" → LevelDebug, "info" → LevelInfo, "warn"/"warning" → LevelWarn, "error" → LevelError. Valor desconocido o vacío → LevelInfo (safe default).
**Motivo**: Separación de responsabilidades: config provee el string, logging lo parsea. Safe default evita silenciar logs accidentalmente.
**Ámbito**: Inicialización del servidor en `cmd/server/main.go` al construir el logger.
**Alternativas**: Usar `slog.Level` directamente en config (menos flexible para variables de entorno).
**Ejemplo**: `logger := logging.New(logging.ParseLevel(cfg.LogLevel))`

## Regla 3: Logger inyectado vía contexto (nunca global mutable)
**Contexto**: Handlers y workers necesitan acceso al logger configurado (con nivel correcto). Usar `slog.SetDefault()` es global mutable y problemático en tests.
**Decisión**: Inyectar el logger en `context.Context` vía `logging.WithContext(ctx, logger)`. Los handlers lo extraen con `logging.FromContext(ctx)` que retorna `slog.Default()` si no hay logger en contexto (nunca nil).
**Motivo**: Inmutabilidad, testabilidad (cada test puede inyectar su logger), evita efectos secundarios globales.
**Ámbito**: Todos los handlers HTTP, workers, y componentes que necesiten loguear.
**Alternativas**: Variable global `var Log *slog.Logger` (mutable, problemático en tests), parámetro explícito en cada función (verboso).
**Ejemplo**: `logger := logging.FromContext(r.Context()); logger.Info("event received", "type", eventType)`
