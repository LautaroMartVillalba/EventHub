---
topic: go-naming-shadowing
expert: Exp-Backend
date: 2026-07-30
scope: backend
source: decision
status: active
---

## Regla 1: Evitar shadowing de nombre de package al renombrar variables
**Contexto**: Al expandir la variable `cfg` a `config` en cmd/server/main.go, la nueva variable `config` sombreó el package importado `config`, causando que el calificador `config.Load()` se volviera ambiguo.
**Decisión**: Cuando una variable deba nombrarse igual que el package que usa, anteponer un prefijo descriptivo como `appConfig` en lugar de usar el nombre exacto del package.
**Motivo**: Go permite variables que sombrean packages, pero `go vet -shadow` lo señala como warning y reduce la legibilidad. El prefijo `app` desambigua sin perder información.
**Ámbito**: Archivos que importan un package y declaran una variable que coincidiría con su nombre.
**Alternativas**: (a) Mantener la abreviatura original (p. ej. `cfg`), (b) usar un prefijo como `app`, (c) usar alias de import como `config "eventhub/internal/config"` (no evita el shadowing).
**Ejemplo**:
```go
// ❌ Shadowing: variable config sombrea package config
config, err := config.Load()
fmt.Println(config.HTTPPort)

// ✅ Correcto: prefijo desambiguante
appConfig, err := config.Load()
fmt.Println(appConfig.HTTPPort)
```

## Regla 2: Evitar shadowing de tipo de interfaz al renombrar parámetros
**Contexto**: Al expandir el parámetro `s` a `scanner` en las funciones `scanEvent(s scanner)` y `scanProcess(s scanner)`, el nuevo nombre `scanner` sombreó la interfaz `scanner` definida en el mismo archivo.
**Decisión**: Cuando un parámetro coincida con el nombre de un tipo de interfaz, usar un nombre descriptivo que no opaque al tipo — p. ej. `row` para un scanner de filas, `dest` para un writer destino.
**Motivo**: El shadowing de tipo hace que el tipo original sea inaccesible dentro del cuerpo de la función. Aunque Go lo permita, los linters estáticos y el código resultante son más frágiles.
**Ámbito**: Funciones cuyo parámetro de interfaz tiene el mismo nombre que la interfaz misma.
**Alternativas**: (a) Usar un sinónimo descriptivo como `row`, (b) conservar la abreviatura corta `s` que es idiomática en Go para scanners, (c) renombrar la interfaz a un nombre más específico como `rowScanner`.
**Ejemplo**:
```go
// ❌ Shadowing: parámetro scanner sombrea tipo scanner
func scanEvent(scanner scanner) (domain.Event, error) {
    err := scanner.Scan(...) // scanner aquí es el parámetro, no la interfaz
}

// ✅ Correcto: row es descriptivo y no sombrea
func scanEvent(row scanner) (domain.Event, error) {
    err := row.Scan(...)
}

// ✅ Alternativa: mantener abreviatura corta
func scanEvent(s scanner) (domain.Event, error) {
    err := s.Scan(...)
}
```

## Regla 3: Preservar nombres cortos idiomáticos de Go
**Contexto**: Durante un refactor masivo de expansión de nombres, algunos identificadores cortos son estándar en Go y deben preservarse por convención de la comunidad.
**Decisión**: Mantener sin expandir los siguientes nombres cortos Go: `err` (error), `ctx` (context), `id` (identifier), `ok` (boolean ok), `i/j/k/n` (loop indices), `wg` (sync.WaitGroup), `mu` (sync.Mutex), `tx` (*sql.Tx), `db` (*sql.DB), `t` (*testing.T), `w` (http.ResponseWriter), `r` (*http.Request).
**Motivo**: Estos nombres son estándar en la comunidad Go y expandirlos reduce la legibilidad para desarrolladores familiarizados con el lenguaje.
**Ámbito**: Cualquier refactor de nombres en código Go.
**Alternativas**: Expandir todo (reduce legibilidad para Gophers).
