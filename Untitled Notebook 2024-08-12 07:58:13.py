# Databricks notebook source
dbutils.fs.cp("dbfs:/Workspace/Users/jupahefi@gmail.com/constants.py", "dbfs:/FileStore/modules/constants.py")
sys.path.append("/dbfs/FileStore/modules")
import constants

# COMMAND ----------

# MAGIC %run "/Workspace/Users/jupahefi@gmail.com/constants.py"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pwd

# COMMAND ----------

with open("/Workspace/Users/jupahefi@gmail.com/constants.py") as f:
    code = f.read()

    exec(code)

# COMMAND ----------

hi_amit()
