# Databricks notebook source
# MAGIC %md
# MAGIC #Enrollment Object

# COMMAND ----------

# MAGIC %md
# MAGIC The "Paletable" object, Enrollment.

# COMMAND ----------

from palet.Enrollment import Enrollment

# COMMAND ----------

# MAGIC %md
# MAGIC ####Enrollment by Year (2020-2021)

# COMMAND ----------

# MAGIC %md
# MAGIC I want to get an enrollment breakdown

# COMMAND ----------

#api = Enrollment()
api = Enrollment().byMonth().byState()

# COMMAND ----------

## This is likely becoming deprecated (i.e. setting runids)
api.runids = [5149,6297]

# COMMAND ----------

print(api.sql())

# COMMAND ----------

# MAGIC %md
# MAGIC What does the API give me?

# COMMAND ----------

display(api.fetch())

# COMMAND ----------

# MAGIC %md
# MAGIC How did it give me these results?

# COMMAND ----------

print(api.sql())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Enrollment by Month (2020-2021)

# COMMAND ----------

# MAGIC %md
# MAGIC Can I drill down by month?

# COMMAND ----------

display(api.byMonth().fetch()) 

# COMMAND ----------

# MAGIC %md
# MAGIC How does this query differ from my previous one?

# COMMAND ----------

print(api.byMonth().sql())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Enrollment by Gender (Female)
# MAGIC Now, I want to see Enrollment, by month and state focusing specifically on female beneficiaries 

# COMMAND ----------

display(api.byGender('F').fetch())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Enrollment by State (All)
# MAGIC I want to go through this again focusing on individual states, let's return to the beginning
