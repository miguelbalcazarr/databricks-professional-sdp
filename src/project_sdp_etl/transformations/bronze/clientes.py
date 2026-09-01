from pyspark.sql.functions import col, lit, current_timestamp
from pyspark import pipelines as dp
from project_sdp.src.project_sdp_etl.schemas.bronze.clientes import schema_clientes


@dp.table(
    name="clientes_raw",
    comment="Tabla Bronze clientes_raw",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false",
        "delta.appendOnly": "true",
    },
)

def bronze_table():
    df_reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("delimiter", ",")
        .schema(schema_clientes())
        .load("/Volumes/sdp/default/landing")
        .withColumn("ingest_at", current_timestamp())
        .withColumn("metadata", col("_metadata"))
    )

    return df_reader