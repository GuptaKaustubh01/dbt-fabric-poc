# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0a642a71-9c8a-40b5-a375-11b55f7e75aa",
# META       "default_lakehouse_name": "dbt_demo_lakehouse",
# META       "default_lakehouse_workspace_id": "c2466ec4-c68c-460a-802c-3845904cf05a",
# META       "known_lakehouses": [
# META         {
# META           "id": "0a642a71-9c8a-40b5-a375-11b55f7e75aa"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

workspace_id = ""
lakehouse_id = ""
schema_name = ""
source_name = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{schema_name}"

table_paths = mssparkutils.fs.ls(source_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t in table_paths:

    table_name = t.name.replace("/", "")

    if table_name.startswith("_"):
        continue

    src_table_path = f"{source_path}/{table_name}"
    final_schema_name = f"{source_name}_{schema_name}"
    target_table = f"{final_schema_name}.{table_name}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {final_schema_name}")

    df = spark.read.format("delta").load(src_table_path)

    (
        df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
