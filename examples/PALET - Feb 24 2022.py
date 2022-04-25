# Databricks notebook source
# MAGIC %md
# MAGIC #### PALET Sprint Demo: 
# MAGIC 1. Income Bracket by Group
# MAGIC 2. Age Range by Group
# MAGIC 3. Eligible but Not Enrolled
# MAGIC 4. Enhanced Class Function Logging

# MAGIC %md
# MAGIC Start by importing Enrollment, Eligibility, and PaletMetadata from PALET.

# COMMAND ----------

from palet.Enrollment import Enrollment
from palet.Eligibility import Eligibility
from palet.PaletMetadata import PaletMetadata


# COMMAND ----------

# MAGIC %md
# MAGIC #### New by Groups - byIncomeBracket(), byAgeRange(), and byEnrollmentType

# COMMAND ----------

# MAGIC %md
# MAGIC Let's start by creating a an enrollment object. 

# COMMAND ----------

api = Enrollment().byMonth()
type(api)

# COMMAND ----------

# MAGIC %md
# MAGIC I want to view enrollment broken down by the predetermined age groups (age_grp_flag) with in the TAF data.

# COMMAND ----------

df = api.byAgeRange().fetch()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC This default option provides a helpful breakdown of different age groups, but what if I want to view age bands that aren't already within the TAF data?
# MAGIC 
# MAGIC In this case, we will enter a python dictionary as an arguement, and specify user defined age bands. Simply enter a string as a label, then a list of integers specifying a minimum and maximum range.

# COMMAND ----------

df = api.byAgeRange({'Teenager': [13,19],'Adult': [20,64],'Senior': [65,125]}).fetch()

display(df)

# COMMAND ----------

print(api.sql())

# COMMAND ----------

# MAGIC %md
# MAGIC In this example we use 3 different age bands, but this isn't required. The user has the flexibilty to enter as few or as many age bands as they desire. 

# COMMAND ----------

# MAGIC %md
# MAGIC Continuing with these user defined age bands, let's also break our enrollment query out by income bracket.

# COMMAND ----------

df = api.byIncomeBracket().fetch()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### View Eligible Beneficiaries Who Aren't Enrolled

# COMMAND ----------

# MAGIC %md
# MAGIC Let's shift out focus to eligibility. The first step here is to call our original enrollment object as an eligibility object. 

# COMMAND ----------

api = Eligibility(api)
type(api)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have confirmed our enrollment object is converted to an eligibility object, let's view counts for those who are eligible but not enrolled. 

# COMMAND ----------

display(api.notEnrolled().fetch())
