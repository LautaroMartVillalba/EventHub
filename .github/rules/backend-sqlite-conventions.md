---
topic: backend-sqlite-conventions
expert: Exp-Backend
date: 2026-07-29
scope: backend
source: decision
status: active
---

## Regla 1: Usar DSN `_pragma` en vez de `db.Exec` para PRAGMAs
**Contexto**: `database/sql` mantiene un pool de conexiones. `db.Exec("PRAGMA foreign_keys = ON")` solo afecta la conexión actual, no las futuras.
**Decisión**: Configurar PRAGMAs via parámetros `_pragma` en la DSN de conexión SQLite.
**Motivo**: modernc.org/sqlite aplica `_pragma` a cada nueva conexión automáticamente, garantizando consistencia.
**Ámbito**: Todas las conexiones SQLite en el proyecto.
**Alternativas**: `db.SetMaxOpenConns(1)` (mata concurrencia), ejecutar PRAGMA en cada transacción (repetitivo).
**Ejemplo**: `sql.Open("sqlite", "file:path?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)")`

## Regla 2: No usar CGO — mantener modernc.org/sqlite
**Contexto**: El proyecto no tiene ni usará CGO.
**Decisión**: Usar exclusivamente `modernc.org/sqlite` como driver SQLite.
**Motivo**: Zero dependencias C, compilación cruzada sencilla, despliegue simple.
**Ámbito**: go.mod y todas las importaciones de SQLite.

## Regla 3: Timestamps en UTC en Go, datetime('now') en SQL
**Contexto**: Necesidad de consistencia temporal entre capas.
**Decisión**: En constructores Go usar `time.Now().UTC()`. En updates SQL usar `datetime('now')`.
**Motivo**: Evitar ambigüedad de zonas horarias. SQLite no tiene timezone info.
**Ámbito**: internal/domain, internal/storage
