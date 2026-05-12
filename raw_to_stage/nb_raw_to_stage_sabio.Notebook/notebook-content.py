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

# Import Fabric/Synapse notebook utilities for file system, secrets, and environment operations
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Define Dynamic Runtime Parameters

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

# Construct OneLake ABFSS path dynamically for the given workspace, lakehouse, and schema
source_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{schema_name}"

# List all tables/files available in the specified schema directory
table_paths = mssparkutils.fs.ls(source_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Ingest and Replicate Tables Across Lakehouses
# 
# This step iterates through all discovered tables in the source schema and copies them into the target workspace/lakehouse structure.
# 
# #### Operations Performed
# - Iterates through all tables in the source schema directory
# - Filters out system/internal tables (names starting with `_`)
# - Constructs source and target table paths dynamically
# - Creates target schema if it does not exist
# - Reads Delta tables from source lakehouse
# - Writes data into target lakehouse as managed Delta tables
# 
# #### Key Features
# - Fully metadata-driven ingestion
# - Dynamic schema creation per source
# - Overwrites target tables with latest data
# - Preserves Delta format for consistency

# CELL ********************

# Iterate through all tables discovered in source schema path
for t in table_paths:

    # Extract clean table name by removing trailing slash
    table_name = t.name.replace("/", "")

    # Skip system/internal metadata folders
    if table_name.startswith("_"):
        continue

    # Construct full source table path in OneLake
    src_table_path = f"{source_path}/{table_name}"

    # Define target schema and fully qualified table name
    final_schema_name = f"{source_name}_{schema_name}"
    target_table = f"{final_schema_name}.{table_name}"

    # Ensure target schema exists in the workspace
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {final_schema_name}")

    # Read source Delta table
    df = spark.read.format("delta").load(src_table_path)

    # Write data into target as managed Delta table (overwrite mode)
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
