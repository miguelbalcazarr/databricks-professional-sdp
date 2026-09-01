from pyspark import pipelines as dp
from pyspark.sql.functions import col, date_trunc, count


@dp.materialized_view(
    name="dbassociate.gold.clientes_resumen_mensual",
    comment="Tabla Gold: conteo de clientes por ciudad y mes de registro",
    table_properties={"quality": "gold"},
)
def gold_clientes_resumen_mensual():
    df = spark.read.table("dbassociate.silver.clientes")

    return (
        df.filter(col("ciudad").isNotNull() & col("fecha_registro").isNotNull())
        .withColumn("mes_registro", date_trunc("month", col("fecha_registro")))
        .groupBy("ciudad", "mes_registro")
        .agg(count("*").alias("total_clientes"))
    )
