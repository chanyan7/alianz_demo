# allianz_demo

It is a demo to develop an ETL pipeline such that extracting dataa from CSV and loading to SQL DB (Postgres)
  - Carga incremental desde ultima ejecucción
  - registra logs en tabla logs de BD
  - guarda los registros errorneos (missing values) en una tabla aparte.
  - Dato Customer_id esta encriptado
  - config.ini guarda los parametros: conexion DB (password encriptado), path de CSV...
