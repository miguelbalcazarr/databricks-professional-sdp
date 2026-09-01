# Datos fuente sintéticos — clientes

CSVs generados para reemplazar el landing zone (`/Volumes/sdp/default/landing`) que
lee `bronze/clientes.py` vía Auto Loader. Reproducibles con
`data/raw/clientes/gen_clientes.py` si quieres regenerarlos o ajustar volumen/errores.

## Lotes

| Archivo | Filas | Contenido |
|---|---|---|
| `lote_01_clientes_2024-01-15.csv` | 40 | Clientes nuevos `id_cliente` 1–39 + 1 fila con `id_cliente` vacío |
| `lote_02_clientes_2024-02-12.csv` | 29 | Clientes nuevos 40–59 + 1 fila con fecha no parseable + 8 actualizaciones a IDs del lote 1 |
| `lote_03_clientes_2024-03-10.csv` | 23 | Clientes nuevos 60–74 + 1 fila con fecha futura + 1 fila con fecha < 1900 + 6 actualizaciones (algunas sobre IDs ya actualizados en el lote 2) |

Cada lote representa un "drop" de archivos en el volumen. Súbelos **en orden** y deja
que el pipeline procese cada uno (Auto Loader es incremental) para ver:

- **Ingesta incremental** en `clientes_raw` (Bronze) lote a lote.
- **AUTO CDC tipo 1** en `clientes` (Silver): las actualizaciones de los lotes 2 y 3
  deben pisar el estado anterior del mismo `id_cliente` (mismo nombre, ciudad/email/fecha
  nuevos).
- Las 3 `expectations` (warning, no descartan filas) en `view_clientes`:
  - `id_cliente` nulo
  - email inválido/ausente
  - fecha inválida o fuera de rango (`< 1900-01-01` o `> hoy`)
- La capa **Gold** (`clientes_resumen_mensual`): conteo de clientes por ciudad y mes.

## Cómo subirlos al volumen (dev)

```bash
databricks fs cp data/raw/clientes/lote_01_clientes_2024-01-15.csv \
  dbfs:/Volumes/sdp/default/landing/lote_01_clientes_2024-01-15.csv --profile <PROFILE>
```

Repite para cada lote, uno a la vez, corriendo el pipeline entre cada subida.

**Nota:** el volumen `sdp.default.landing` debe existir de antemano — el código de
`bronze/clientes.py` no lo crea. Y en `databricks.yml`, las variables `catalog_prd` /
`schema_bronze_prd` (usadas por el pipeline) no están sobreescritas por target, así que
el pipeline de `dev` también publica en `sdp_prd.bronze` salvo que ajustes el bundle.

## ⚠️ Gotcha esperado: la fila con `id_cliente` vacío

`lote_01` incluye a propósito una fila con `id_cliente` vacío para poder ver el warning
`warning_id_cliente_null`. Pero el schema de Silver declara `id_cliente BIGINT NOT NULL`
y la expectation es solo `expect_all` (warn, no descarta la fila) — así que esa fila
**probablemente hará fallar el `AUTO CDC` merge** al intentar escribir un `id_cliente`
nulo en una columna `NOT NULL`, en vez de solo generar un warning.

Es un gap real del pipeline (expectation de solo-warning sobre la clave de merge de una
tabla NOT NULL), útil como punto de discusión en el laboratorio. Dos salidas:

- Cambiar esa constraint puntual a `@dp.expect_or_drop("warning_id_cliente_null", ...)`
  para descartar filas con `id_cliente` nulo antes del CDC.
- O quitar esa fila del CSV si prefieres una corrida sin fallas para la demo inicial.
