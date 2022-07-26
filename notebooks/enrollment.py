# Databricks notebook source
# MAGIC %md
# MAGIC ##### link to the documentation
# MAGIC https://cms-dataconnect.atlassian.net/wiki/spaces/PAL/pages/24913281261/PALET+Wiki+v.+1.6.20220325

# COMMAND ----------

# MAGIC %md
# MAGIC https://tmsis.github.io/palet/Diagnoses.html

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import the Paletable Objects

# COMMAND ----------

import sys
sys.path.append('/dbfs/FileStore/shared_uploads/akira/lib')

from palet.DateDimension import DateDimension
from palet.Enrollment import Enrollment
from palet.Diagnoses import Diagnoses
from palet.ServiceCategory import ServiceCategory
from palet.Readmits import Readmits
from palet.EligibilityType import EligibilityType
from palet.CoverageType import CoverageType

# COMMAND ----------

DateDimension(years=[2019,2020,2021])

# COMMAND ----------

api = Enrollment()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enrollment by year

# COMMAND ----------

api.timeunit = 'year'

# COMMAND ----------

# Are these correct enrollments? How do we read them?

display(api.fetch())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Enrollment By Year

# COMMAND ----------

api.counter = 'full'

# COMMAND ----------

display(api.fetch())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partial Enrollment

# COMMAND ----------

pe = Enrollment()

# COMMAND ----------

pe.counter = 'partial'

# COMMAND ----------

display(pe.byYear().fetch())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enrollment by Year, by State

# COMMAND ----------

display(api.byState().fetch())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enrollment by Year, by State, by Gender

# COMMAND ----------

display(api.byGender().fetch())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enrollment by Year, by State, by Gender, by Coverage Type

# COMMAND ----------

#This command runs slowly.
display(api.byCoverageType().fetch())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enrollment by Year & Coverate Type, in Alabama, for Females Only 

# COMMAND ----------

# display(api.byState(‘Alabama’).fetch()) # curly quotes ’ copied from documentation are tricky to find when they cause trouble 
# Thank you, Matt.
display(api.byState('AL').byGender('F').fetch())


# COMMAND ----------

# MAGIC %md
# MAGIC ##Testing Eligibility Type and Coverage Type

# COMMAND ----------

display(Enrollment().byCoverageType().byState(['MD']).byYear().fetch())

# COMMAND ----------

display(Enrollment().byCoverageType().byEligibilityType().byState(['MD']).byYear().fetch())

# COMMAND ----------

display(Enrollment().byCoverageType(['01']).byEligibilityType(['01']).byState(['MD']).byYear().fetch())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Readmissions

# COMMAND ----------

from palet.Readmits import Readmits

# COMMAND ----------

readm_30 = Enrollment().having(Readmits.allcause(30))

# COMMAND ----------

readm_60 = Enrollment().having(Readmits.allcause(60))

# COMMAND ----------

display(readm_30.fetch())

# COMMAND ----------

display(readm_30.byYear().byState().fetch())

# COMMAND ----------

display(Enrollment().byState(['MD']).byGender('F').byYear().having(Readmits.allcause(30)).fetch())

# COMMAND ----------

# ERROR
display(Enrollment().byState(['MD']).byGender('F').byYear().having(Readmits.allcause(30)).fetch())

# COMMAND ----------

# ERROR
display(Enrollment().byState(['MD']).byGender('F').byYear().calculate(Readmits.allcause(30)).fetch())

# COMMAND ----------

# ERROR
display(Enrollment().byState(['MD']).byGender('F').calculate(Readmits.allcause(30)).fetch())

# COMMAND ----------

enrl = Enrollment().byState(['MD']).byGender('F').calculate(Readmits.allcause(30))

# COMMAND ----------

enrl.fetch()

# COMMAND ----------

# dis_MD = Enrollment().byYear().mark(Readmits.allcause(30), 'readmits').fetch()
dis_MD = Enrollment().byState(['MD']).byGender('F').byYear().mark(Readmits.allcause(30), 'readmits')

# COMMAND ----------

df_dis_MD = dis_MD.fetch()

# COMMAND ----------

from palet.Enrollment import Enrollment

# COMMAND ----------

dsn_MD = Enrollment().byState('MD').byGender('F').having(Readmits.allcause(30))

# COMMAND ----------

display(readm_30.byYear().byState(['MD']).byGender('F').fetch())

# COMMAND ----------

display(readm_60.fetch())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enrollment by Year & Month

# COMMAND ----------

api = Enrollment()
display(api.byMonth().fetch())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enrollment-Full or Partial Month

# COMMAND ----------

# A bug: the same numbers for full and partial show up. 
# api = Enrollment(period='partial')
# fix:
api = Enrollment(counter='partial')

# COMMAND ----------

display(api.byYear().fetch())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show me the SQL Code

# COMMAND ----------

print(api.sql())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Coverage Type Explained

# COMMAND ----------

from palet.CoverageType import CoverageType

# COMMAND ----------

CoverageType.alias

# COMMAND ----------

CoverageType.cols

# COMMAND ----------

print(api.sql())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Atrial fibrillation (AF)

# COMMAND ----------

AFib = ['I230', 'I231', 'I232', 'I233', 'I234', 'I235', 'I236', 'I237', 'I238', 'I213', 'I214', 'I219', 'I220', 'I221', 'I222', 'I228', 'I229', 'I21A1', 'I21A9', 'I2101', 'I2102', 'I2109', 'I2111', 'I2119', 'I2121', 'I2129']

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show Chronic Condtion

# COMMAND ----------

diag = Diagnoses.within([(ServiceCategory.inpatient, 1)], AFib, 2)

# COMMAND ----------

print(diag.sql())

# COMMAND ----------

date_dim = DateDimension.getInstance()

# COMMAND ----------

display(date_dim.df)

# COMMAND ----------

display(api.having(Diagnoses.within([(ServiceCategory.inpatient, 1)], AFib, 2)).fetch())

# COMMAND ----------

display(Enrollment(counter='partial').byCoverageType().mark(Diagnoses.within([(ServiceCategory.inpatient, 1)], AFib, 2), 'AFib').fetch())

# COMMAND ----------

Diagnoses.inpatient

# COMMAND ----------

api = Enrollment().byState()
api.timeunit = 'year'
[api.by_group, api.timeunit]

# COMMAND ----------

print(api.sql())

# COMMAND ----------

display(api.fetch())

# COMMAND ----------

# api = Enrollment(period='partial').fetch()
# api = Enrollment(period='partial').byEnrollmentType()

# COMMAND ----------

type(api)

# COMMAND ----------

df = Enrollment(counter='partial').fetch()
display(df)

# COMMAND ----------

df.shape

# COMMAND ----------

 api = Enrollment(counter='full')

# COMMAND ----------

display(api.byState().fetch())

# COMMAND ----------

display(api.byMonth().byState().fetch())

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

