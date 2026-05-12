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

# MARKDOWN ********************

# ### Initialize Fabric Environment Utilities#

# CELL ********************

# %run nb_fabric_environment

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import uuid module for generating unique identifiers
import uuid

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Define Source-to-Lakehouse Mapping Configuration

# CELL ********************

data = [
    {
        "source_name": "t1",
        "workspace_name": "test_workspace_1",
        "lakehouse_name": "test_lakehouse_1",
        "schema_name": "test_1",
        "workspace_id": "7978a990-0dc5-4d56-832a-b7a32831da96",
        "lakehouse_id": "49d9210d-2c30-4220-a2d4-06aa31bf749a",
        "is_active": 1
    },
    {
        "source_name": "t2",
        "workspace_name": "test_workspace_2",
        "lakehouse_name": "test_lakehouse_2",
        "schema_name": "test_2",
        "workspace_id": "85ea4be3-33ec-480d-aeb9-8b0e8a290be4",
        "lakehouse_id": "afffaeac-86f1-4896-b054-51824547f9ac",
        "is_active": 1
    }
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform configuration data into structured metadata rows for table insertion
rows = []

for item in data:
    source_name = item["source_name"]
    ws_name = item["workspace_name"]
    lh_name = item["lakehouse_name"]
    schema_name = item["schema_name"]
    is_active = item["is_active"]
    
    workspace_id = item["workspace_id"]
    lakehouse_id = item["lakehouse_id"]

    # workspace_id = get_workspace_id_by_name(ws_name)
    # lakehouse_id = get_fabric_item_id(lh_name, ws_name)

    rows.append({
        "meta_id": str(uuid.uuid4()),
        "source_name": source_name,
        "workspace_name": ws_name,
        "lakehouse_name": lh_name,
        "schema_name": schema_name,
        "workspace_id": workspace_id,
        "lakehouse_id": lakehouse_id,
        "is_active": is_active
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert prepared metadata rows into Spark DataFrame
df = spark.createDataFrame(rows)

# Create temporary view for SQL-based processing and staging
df.createOrReplaceTempView("meta_stage_view")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Create Delta Table for Workspace-Lakehouse Mapping

# CELL ********************

# Drop existing mapping table if present to ensure clean recreation
spark.sql("""
DROP TABLE IF EXISTS meta_workspace_lakehouse_mapping
""")

# Create Delta table for storing workspace-lakehouse metadata mappings
spark.sql("""
CREATE TABLE meta_workspace_lakehouse_mapping (
    meta_id STRING,
    source_name STRING,
    workspace_name STRING,
    lakehouse_name STRING,
    schema_name STRING,
    workspace_id STRING,
    lakehouse_id STRING,
    is_active INT
)
USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
INSERT OVERWRITE TABLE meta_workspace_lakehouse_mapping
SELECT 
    meta_id,
    source_name,
    workspace_name,
    lakehouse_name,
    schema_name,
    workspace_id,
    lakehouse_id,
    is_active
FROM meta_stage_view
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
