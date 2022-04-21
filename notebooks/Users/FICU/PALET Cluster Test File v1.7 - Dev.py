# Databricks notebook source
# MAGIC %md
# MAGIC #### Cluster Test File
# MAGIC 
# MAGIC Use Case: Run this file once a new version of PALET has been pushed to their respective cluster on either the PROD or VAL Servers
# MAGIC 
# MAGIC Purpose: Does not return dataframes, this file is used to confirm that our assertations about the PALET library are correct. 
# MAGIC 
# MAGIC Functionality Testing:
# MAGIC 1. Paletable objects i.e. Enrollment, Eligibility, etc.
# MAGIC 2. Counts for enrollment
# MAGIC 3. By groups i.e. byMonth(), byState(), etc.
# MAGIC 4. Time units  i.e. month, year, full month, partial month
# MAGIC 5. Filtering by Chronic Conidition

# COMMAND ----------

from palet.Enrollment import Enrollment
from palet.Eligibility import Eligibility
import pandas as pd
from palet.Palet import Palet
from palet.Diagnoses import Diagnoses
from palet.ServiceCategory import ServiceCategory

# COMMAND ----------

inc = Enrollment()
display(inc.byAgeRange({'Teenager': [13,19],'Twenties': [20,29],'Thirties': [30,39]}).fetch()) 

# COMMAND ----------

Palet.showReleaseNotes()

# COMMAND ----------

print(inc.sql())

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   counter,
# MAGIC   age_band,
# MAGIC   de_fil_dt as year,
# MAGIC   month,
# MAGIC   sum(mdcd_enrollment) as mdcd_enrollment,
# MAGIC   sum(chip_enrollment) as chip_enrollment
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       aa.de_fil_dt,
# MAGIC       'In Month' as counter,
# MAGIC       stack(
# MAGIC         12,
# MAGIC         1,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_01 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_01 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         2,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_02 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_02 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         3,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_03 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_03 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         4,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_04 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_04 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         5,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_05 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_05 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         6,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_06 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_06 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         7,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_07 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_07 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         8,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_08 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_08 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         9,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_09 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_09 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         10,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_10 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_10 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         11,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_11 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_11 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         12,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_12 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_12 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         )
# MAGIC       ) as (month, mdcd_enrollment, chip_enrollment),
# MAGIC       case
# MAGIC         when age_num >= 13
# MAGIC         and age_num <= 19 then 'Teenager'
# MAGIC         when age_num >= 20
# MAGIC         and age_num <= 29 then 'Twenties'
# MAGIC         when age_num >= 30
# MAGIC         and age_num <= 39 then 'Thirties'
# MAGIC         else 'not found'
# MAGIC       end as age_band
# MAGIC     from
# MAGIC       taf.taf_ann_de_base as aa
# MAGIC     where
# MAGIC       aa.da_run_id in (7256)
# MAGIC       and (
# MAGIC         (
# MAGIC           (aa.mdcd_enrlmt_days_01 > 0)
# MAGIC           or (aa.chip_enrlmt_days_01 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_02 > 0)
# MAGIC           or (aa.chip_enrlmt_days_02 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_03 > 0)
# MAGIC           or (aa.chip_enrlmt_days_03 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_04 > 0)
# MAGIC           or (aa.chip_enrlmt_days_04 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_05 > 0)
# MAGIC           or (aa.chip_enrlmt_days_05 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_06 > 0)
# MAGIC           or (aa.chip_enrlmt_days_06 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_07 > 0)
# MAGIC           or (aa.chip_enrlmt_days_07 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_08 > 0)
# MAGIC           or (aa.chip_enrlmt_days_08 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_09 > 0)
# MAGIC           or (aa.chip_enrlmt_days_09 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_10 > 0)
# MAGIC           or (aa.chip_enrlmt_days_10 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_11 > 0)
# MAGIC           or (aa.chip_enrlmt_days_11 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_12 > 0)
# MAGIC           or (aa.chip_enrlmt_days_12 > 0)
# MAGIC         )
# MAGIC       )
# MAGIC       and 1 = 1
# MAGIC     group by
# MAGIC       age_band,
# MAGIC       aa.de_fil_dt
# MAGIC     order by
# MAGIC       age_band,
# MAGIC       aa.de_fil_dt
# MAGIC   )
# MAGIC where
# MAGIC   ((1 in (1)))
# MAGIC group by
# MAGIC   counter,
# MAGIC   age_band,
# MAGIC   de_fil_dt,
# MAGIC   month
# MAGIC order by
# MAGIC   age_band,
# MAGIC   de_fil_dt,
# MAGIC   month

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paletable Objects

# COMMAND ----------

enrollapi = Enrollment([7256,7255])

assert str(type(enrollapi)) == "<class 'palet.Enrollment.Enrollment'>", 'The Paletable object is not an Enrollment object.'
print('The Paletable object is an Enrollment object.')

# COMMAND ----------

enrollapi.byEnrollmentType(['1', '4'])
df = enrollapi.fetch()

# COMMAND ----------

print(enrollapi.sql())

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   counter,
# MAGIC   coverage_type,
# MAGIC   de_fil_dt as year,
# MAGIC   month,
# MAGIC   sum(mdcd_enrollment) as mdcd_enrollment,
# MAGIC   sum(chip_enrollment) as chip_enrollment
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       aa.de_fil_dt,
# MAGIC       'In Month' as counter,
# MAGIC       stack(
# MAGIC         12,
# MAGIC         1,
# MAGIC         aa.mc_plan_type_cd_01,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_01 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_01 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         2,
# MAGIC         aa.mc_plan_type_cd_02,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_02 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_02 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         3,
# MAGIC         aa.mc_plan_type_cd_03,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_03 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_03 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         4,
# MAGIC         aa.mc_plan_type_cd_04,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_04 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_04 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         5,
# MAGIC         aa.mc_plan_type_cd_05,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_05 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_05 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         6,
# MAGIC         aa.mc_plan_type_cd_06,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_06 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_06 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         7,
# MAGIC         aa.mc_plan_type_cd_07,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_07 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_07 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         8,
# MAGIC         aa.mc_plan_type_cd_08,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_08 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_08 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         9,
# MAGIC         aa.mc_plan_type_cd_09,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_09 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_09 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         10,
# MAGIC         aa.mc_plan_type_cd_10,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_10 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_10 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         11,
# MAGIC         aa.mc_plan_type_cd_11,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_11 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_11 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         12,
# MAGIC         aa.mc_plan_type_cd_12,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_12 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_12 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         )
# MAGIC       ) as (
# MAGIC         month,
# MAGIC         coverage_type,
# MAGIC         mdcd_enrollment,
# MAGIC         chip_enrollment
# MAGIC       )
# MAGIC     from
# MAGIC       taf.taf_ann_de_base as aa
# MAGIC     where
# MAGIC       aa.da_run_id in (7256)
# MAGIC       and (
# MAGIC         (
# MAGIC           (aa.mdcd_enrlmt_days_01 > 0)
# MAGIC           or (aa.chip_enrlmt_days_01 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_02 > 0)
# MAGIC           or (aa.chip_enrlmt_days_02 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_03 > 0)
# MAGIC           or (aa.chip_enrlmt_days_03 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_04 > 0)
# MAGIC           or (aa.chip_enrlmt_days_04 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_05 > 0)
# MAGIC           or (aa.chip_enrlmt_days_05 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_06 > 0)
# MAGIC           or (aa.chip_enrlmt_days_06 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_07 > 0)
# MAGIC           or (aa.chip_enrlmt_days_07 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_08 > 0)
# MAGIC           or (aa.chip_enrlmt_days_08 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_09 > 0)
# MAGIC           or (aa.chip_enrlmt_days_09 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_10 > 0)
# MAGIC           or (aa.chip_enrlmt_days_10 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_11 > 0)
# MAGIC           or (aa.chip_enrlmt_days_11 > 0)
# MAGIC         )
# MAGIC         or (
# MAGIC           (aa.mdcd_enrlmt_days_12 > 0)
# MAGIC           or (aa.chip_enrlmt_days_12 > 0)
# MAGIC         )
# MAGIC       )
# MAGIC       and 1 = 1
# MAGIC     group by
# MAGIC       mc_plan_type_cd_01,
# MAGIC       mc_plan_type_cd_02,
# MAGIC       mc_plan_type_cd_03,
# MAGIC       mc_plan_type_cd_04,
# MAGIC       mc_plan_type_cd_05,
# MAGIC       mc_plan_type_cd_06,
# MAGIC       mc_plan_type_cd_07,
# MAGIC       mc_plan_type_cd_08,
# MAGIC       mc_plan_type_cd_09,
# MAGIC       mc_plan_type_cd_10,
# MAGIC       mc_plan_type_cd_11,
# MAGIC       mc_plan_type_cd_12,
# MAGIC       aa.de_fil_dt
# MAGIC     order by
# MAGIC       mc_plan_type_cd_01,
# MAGIC       mc_plan_type_cd_02,
# MAGIC       mc_plan_type_cd_03,
# MAGIC       mc_plan_type_cd_04,
# MAGIC       mc_plan_type_cd_05,
# MAGIC       mc_plan_type_cd_06,
# MAGIC       mc_plan_type_cd_07,
# MAGIC       mc_plan_type_cd_08,
# MAGIC       mc_plan_type_cd_09,
# MAGIC       mc_plan_type_cd_10,
# MAGIC       mc_plan_type_cd_11,
# MAGIC       mc_plan_type_cd_12,
# MAGIC       aa.de_fil_dt
# MAGIC   )
# MAGIC where
# MAGIC   (
# MAGIC     (
# MAGIC       (aa.mdcd_enrlmt_days_01 > 0)
# MAGIC       or (aa.chip_enrlmt_days_01 > 0)
# MAGIC       and coverage_type in ('01', '05')
# MAGIC     )
# MAGIC     or (
# MAGIC       (aa.mdcd_enrlmt_days_02 > 0)
# MAGIC       or (aa.chip_enrlmt_days_02 > 0)
# MAGIC       and coverage_type in ('01', '05')
# MAGIC     )
# MAGIC     or (
# MAGIC       (aa.mdcd_enrlmt_days_03 > 0)
# MAGIC       or (aa.chip_enrlmt_days_03 > 0)
# MAGIC       and coverage_type in ('01', '05')
# MAGIC     )
# MAGIC     or (
# MAGIC       (aa.mdcd_enrlmt_days_04 > 0)
# MAGIC       or (aa.chip_enrlmt_days_04 > 0)
# MAGIC       and coverage_type in ('01', '05')
# MAGIC     )
# MAGIC     or (
# MAGIC       (aa.mdcd_enrlmt_days_05 > 0)
# MAGIC       or (aa.chip_enrlmt_days_05 > 0)
# MAGIC       and coverage_type in ('01', '05')
# MAGIC     )
# MAGIC     or (
# MAGIC       (aa.mdcd_enrlmt_days_06 > 0)
# MAGIC       or (aa.chip_enrlmt_days_06 > 0)
# MAGIC       and coverage_type in ('01', '05')
# MAGIC     )
# MAGIC     or (
# MAGIC       (aa.mdcd_enrlmt_days_07 > 0)
# MAGIC       or (aa.chip_enrlmt_days_07 > 0)
# MAGIC       and coverage_type in ('01', '05')
# MAGIC     )
# MAGIC     or (
# MAGIC       (aa.mdcd_enrlmt_days_08 > 0)
# MAGIC       or (aa.chip_enrlmt_days_08 > 0)
# MAGIC       and coverage_type in ('01', '05')
# MAGIC     )
# MAGIC     or (
# MAGIC       (aa.mdcd_enrlmt_days_09 > 0)
# MAGIC       or (aa.chip_enrlmt_days_09 > 0)
# MAGIC       and coverage_type in ('01', '05')
# MAGIC     )
# MAGIC     or (
# MAGIC       (aa.mdcd_enrlmt_days_10 > 0)
# MAGIC       or (aa.chip_enrlmt_days_10 > 0)
# MAGIC       and coverage_type in ('01', '05')
# MAGIC     )
# MAGIC     or (
# MAGIC       (aa.mdcd_enrlmt_days_11 > 0)
# MAGIC       or (aa.chip_enrlmt_days_11 > 0)
# MAGIC       and coverage_type in ('01', '05')
# MAGIC     )
# MAGIC     or (
# MAGIC       (aa.mdcd_enrlmt_days_12 > 0)
# MAGIC       or (aa.chip_enrlmt_days_12 > 0)
# MAGIC       and coverage_type in ('01', '05')
# MAGIC     )
# MAGIC   )
# MAGIC group by
# MAGIC   counter,
# MAGIC   coverage_type,
# MAGIC   de_fil_dt,
# MAGIC   month
# MAGIC order by
# MAGIC   coverage_type,
# MAGIC   de_fil_dt,
# MAGIC   month

# COMMAND ----------

display(df)

# COMMAND ----------

# eligapi = Eligibility()

# assert str(type(eligapi)) == "<class 'palet.Eligibility.Eligibility'>", 'The Paletable object is not an Eligibility object.'
# print('The Paletable object is an Eligibility object.')

# COMMAND ----------

eapi = Enrollment(['7630'])
eapi.byAgeRange({'Teenager': [13,19],'Twenties': [20,29],'Thirties': [30,39]})
print(eapi.sql())

# COMMAND ----------

display(ea
        pi.fetch())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Enrollment Counts
# MAGIC Work in progress

# COMMAND ----------

# MAGIC %md
# MAGIC Yearly Counts

# COMMAND ----------

api = Enrollment().byYear()

df = api.fetch()

# COMMAND ----------

print(api.sql())

# COMMAND ----------

display(df)

# COMMAND ----------

#df = df[['mdcd_enrollment','chip_enrollment']]

#df

# COMMAND ----------

#year = {'mdcd_enrollment':[37533400,87618078,94942910,95748823,94605172,93217105,93339719,97208839],'chip_enrollment':[1680784,3162475,3789852,4259367,4556629,4363985,3974170,3510349]}

# COMMAND ----------

#assert all((df == pd.DataFrame(year))), 'Yearly counts for Medicaid enrollment do not match the expected counts.'
#print('Yearly counts for Medicaid enrollment match the expected counts.')

# COMMAND ----------

# MAGIC %md
# MAGIC #### By Groups - Filters

# COMMAND ----------

# MAGIC %md
# MAGIC Enrollment object with by groups

# COMMAND ----------

df = enrollapi.byMonth().fetch()

df.dtypes

# COMMAND ----------

d = {
  'counter': 'object',
  'year': 'object',
  'month': 'int32',
  'mdcd_enrollment': 'int64',
  'chip_enrollment': 'int64',
  'mdcd_pct_mom': 'float64',
  'chip_pct_mom': 'float64',
  'mdcd_pct_yoy': 'float64',
  'chip_pct_yoy': 'float64'
}
ser = pd.Series(data=d, index=['counter','year','month','mdcd_enrollment','chip_enrollment','mdcd_pct_mom','chip_pct_mom','mdcd_pct_yoy','chip_pct_yoy'])
ser

# COMMAND ----------

assert (ser == df.dtypes).all(), 'The columns or data types returned do not match the expected values.'
print('DataFrame contains the correct columns.')

# COMMAND ----------

df = enrollapi.byRaceEthnicity().fetch()

df.dtypes

# COMMAND ----------

d = {
  'counter': 'object',
  'race_ethncty_flag': 'string',
  'year': 'object',
  'month': 'int32',
  'mdcd_enrollment': 'int64',
  'chip_enrollment': 'int64',
  'mdcd_pct_mom': 'float64',
  'chip_pct_mom': 'float64',
  'mdcd_pct_yoy': 'float64',
  'chip_pct_yoy': 'float64',
  'race': 'object',
}
ser = pd.Series(data=d, index=['counter','race_ethncty_flag','year','month','mdcd_enrollment','chip_enrollment','mdcd_pct_mom','chip_pct_mom','mdcd_pct_yoy','chip_pct_yoy','race'])
ser

# COMMAND ----------

assert (ser == df.dtypes).all(), 'The columns or data types returned do not match the expected values.'
print('DataFrame contains the correct columns.')

# COMMAND ----------

df = enrollapi.byEnrollmentType().fetch()
             
df.dtypes

# COMMAND ----------

print(enrollapi.sql())

# COMMAND ----------

d = {
  'counter': 'object',
  'race_ethncty_flag': 'string',
  'enrollment_type': 'object',
  'year': 'object',
  'month': 'int32',
  'mdcd_enrollment': 'int64',
  'chip_enrollment': 'int64',
  'mdcd_pct_mom': 'float64',
  'chip_pct_mom': 'float64',
  'mdcd_pct_yoy': 'float64',
  'chip_pct_yoy': 'float64',
  'race': 'object',
  'enrollment_type_label': 'object'
}
ser = pd.Series(data=d, index=['counter','race_ethncty_flag','enrollment_type','year','month','mdcd_enrollment','chip_enrollment','mdcd_pct_mom','chip_pct_mom','mdcd_pct_yoy','chip_pct_yoy','race','enrollment_type_label'])
ser

# COMMAND ----------

assert (ser == df.dtypes).all(), 'The columns or data types returned do not match the expected values.'
print('DataFrame contains the correct columns.')

# COMMAND ----------

eapi = Enrollment()
display(eapi.byAgeRange({'Teenager': [13,19],'Twenties': [20,29],'Thirties': [30,39]}).fetch())

# COMMAND ----------

print(eapi.sql())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Eligibility object with by groups

# COMMAND ----------

enrollapi = Enrollment(runIds = ['7256'])
df = enrollapi.byEligibilityType().byState().fetch()
df.dtypes

# COMMAND ----------


print(enrollapi.sql())
display(df)

# COMMAND ----------

d = {
  'counter': 'int32',
  'SUBMTG_STATE_CD': 'string',
  'eligibility_type': 'string',
  'year': 'object',
  'month': 'int32',
  'mdcd_enrollment': 'int64',
  'chip_enrollment': 'int64',
  'mdcd_pct_mom': 'float64',
  'chip_pct_mom': 'float64',
  'mdcd_pct_yoy': 'float64',
  'chip_pct_yoy': 'float64',
  'STNAME': 'object',
  'STABBREV': 'object',
  'eligibility_category': 'object'
}
ser = pd.Series(data=d, index=['counter','SUBMTG_STATE_CD','eligibility_type','year','month','mdcd_enrollment','chip_enrollment',
                               'mdcd_pct_mom', 'chip_pct_mom', 'mdcd_pct_yoy','chip_pct_yoy','STNAME','STABBREV','eligibility_category'])
ser

# COMMAND ----------

assert (ser == df.dtypes).all(), 'The columns or data types returned do not match the expected values.'
print('DataFrame contains the correct columns.')

# COMMAND ----------

df = enrollapi.byGender().fetch()
print(enrollapi.sql())
df.dtypes

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC   %sql
# MAGIC select
# MAGIC   counter,
# MAGIC   SUBMTG_STATE_CD,
# MAGIC   gndr_cd,
# MAGIC   eligibility_type,
# MAGIC   de_fil_dt as year,
# MAGIC   month,
# MAGIC   sum(mdcd_enrollment) as mdcd_enrollment,
# MAGIC   sum(chip_enrollment) as chip_enrollment
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       aa.SUBMTG_STATE_CD,
# MAGIC       aa.gndr_cd,
# MAGIC       aa.de_fil_dt,
# MAGIC       'In Month' as counter,
# MAGIC       stack(
# MAGIC         12,
# MAGIC         1,
# MAGIC         aa.elgblty_grp_cd_01,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_01 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_01 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         2,
# MAGIC         aa.elgblty_grp_cd_02,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_02 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_02 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         3,
# MAGIC         aa.elgblty_grp_cd_03,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_03 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_03 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         4,
# MAGIC         aa.elgblty_grp_cd_04,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_04 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_04 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         5,
# MAGIC         aa.elgblty_grp_cd_05,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_05 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_05 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         6,
# MAGIC         aa.elgblty_grp_cd_06,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_06 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_06 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         7,
# MAGIC         aa.elgblty_grp_cd_07,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_07 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_07 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         8,
# MAGIC         aa.elgblty_grp_cd_08,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_08 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_08 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         9,
# MAGIC         aa.elgblty_grp_cd_09,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_09 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_09 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         10,
# MAGIC         aa.elgblty_grp_cd_10,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_10 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_10 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         11,
# MAGIC         aa.elgblty_grp_cd_11,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_11 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_11 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         12,
# MAGIC         aa.elgblty_grp_cd_12,
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.mdcd_enrlmt_days_12 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         ),
# MAGIC         sum(
# MAGIC           case
# MAGIC             when aa.chip_enrlmt_days_12 > 0 then 1
# MAGIC             else 0
# MAGIC           end
# MAGIC         )
# MAGIC       ) as (
# MAGIC         month,
# MAGIC         eligibility_type,
# MAGIC         mdcd_enrollment,
# MAGIC         chip_enrollment
# MAGIC       )
# MAGIC     from
# MAGIC       taf.taf_ann_de_base as aa
# MAGIC     where
# MAGIC       aa.da_run_id in (7256)
# MAGIC       and (
# MAGIC         (aa.mdcd_enrlmt_days_01 > 0)
# MAGIC         or (aa.chip_enrlmt_days_01 > 0)
# MAGIC         or (aa.mdcd_enrlmt_days_02 > 0)
# MAGIC         or (aa.chip_enrlmt_days_02 > 0)
# MAGIC         or (aa.mdcd_enrlmt_days_03 > 0)
# MAGIC         or (aa.chip_enrlmt_days_03 > 0)
# MAGIC         or (aa.mdcd_enrlmt_days_04 > 0)
# MAGIC         or (aa.chip_enrlmt_days_04 > 0)
# MAGIC         or (aa.mdcd_enrlmt_days_05 > 0)
# MAGIC         or (aa.chip_enrlmt_days_05 > 0)
# MAGIC         or (aa.mdcd_enrlmt_days_06 > 0)
# MAGIC         or (aa.chip_enrlmt_days_06 > 0)
# MAGIC         or (aa.mdcd_enrlmt_days_07 > 0)
# MAGIC         or (aa.chip_enrlmt_days_07 > 0)
# MAGIC         or (aa.mdcd_enrlmt_days_08 > 0)
# MAGIC         or (aa.chip_enrlmt_days_08 > 0)
# MAGIC         or (aa.mdcd_enrlmt_days_09 > 0)
# MAGIC         or (aa.chip_enrlmt_days_09 > 0)
# MAGIC         or (aa.mdcd_enrlmt_days_10 > 0)
# MAGIC         or (aa.chip_enrlmt_days_10 > 0)
# MAGIC         or (aa.mdcd_enrlmt_days_11 > 0)
# MAGIC         or (aa.chip_enrlmt_days_11 > 0)
# MAGIC         or (aa.mdcd_enrlmt_days_12 > 0)
# MAGIC         or (aa.chip_enrlmt_days_12 > 0)
# MAGIC         or (aa.elgblty_grp_cd_01 > 0)
# MAGIC         or (aa.elgblty_grp_cd_02 > 0)
# MAGIC         or (aa.elgblty_grp_cd_03 > 0)
# MAGIC         or (aa.elgblty_grp_cd_04 > 0)
# MAGIC         or (aa.elgblty_grp_cd_05 > 0)
# MAGIC         or (aa.elgblty_grp_cd_06 > 0)
# MAGIC         or (aa.elgblty_grp_cd_07 > 0)
# MAGIC         or (aa.elgblty_grp_cd_08 > 0)
# MAGIC         or (aa.elgblty_grp_cd_09 > 0)
# MAGIC         or (aa.elgblty_grp_cd_10 > 0)
# MAGIC         or (aa.elgblty_grp_cd_11 > 0)
# MAGIC         or (aa.elgblty_grp_cd_12 > 0)
# MAGIC       )
# MAGIC       and 1 = 1
# MAGIC     group by
# MAGIC       aa.SUBMTG_STATE_CD,
# MAGIC       aa.gndr_cd,
# MAGIC       elgblty_grp_cd_01,
# MAGIC       elgblty_grp_cd_02,
# MAGIC       elgblty_grp_cd_03,
# MAGIC       elgblty_grp_cd_04,
# MAGIC       elgblty_grp_cd_05,
# MAGIC       elgblty_grp_cd_06,
# MAGIC       elgblty_grp_cd_07,
# MAGIC       elgblty_grp_cd_08,
# MAGIC       elgblty_grp_cd_09,
# MAGIC       elgblty_grp_cd_10,
# MAGIC       elgblty_grp_cd_11,
# MAGIC       elgblty_grp_cd_12,
# MAGIC       aa.de_fil_dt
# MAGIC     order by
# MAGIC       aa.SUBMTG_STATE_CD,
# MAGIC       aa.gndr_cd,
# MAGIC       elgblty_grp_cd_01,
# MAGIC       elgblty_grp_cd_02,
# MAGIC       elgblty_grp_cd_03,
# MAGIC       elgblty_grp_cd_04,
# MAGIC       elgblty_grp_cd_05,
# MAGIC       elgblty_grp_cd_06,
# MAGIC       elgblty_grp_cd_07,
# MAGIC       elgblty_grp_cd_08,
# MAGIC       elgblty_grp_cd_09,
# MAGIC       elgblty_grp_cd_10,
# MAGIC       elgblty_grp_cd_11,
# MAGIC       elgblty_grp_cd_12,
# MAGIC       aa.de_fil_dt
# MAGIC   )
# MAGIC group by
# MAGIC   counter,
# MAGIC   SUBMTG_STATE_CD,
# MAGIC   gndr_cd,
# MAGIC   eligibility_type,
# MAGIC   de_fil_dt,
# MAGIC   month
# MAGIC order by
# MAGIC   SUBMTG_STATE_CD,
# MAGIC   gndr_cd,
# MAGIC   eligibility_type,
# MAGIC   de_fil_dt,
# MAGIC   month

# COMMAND ----------

d = {
  'SUBMTG_STATE_CD': 'string',
  'gndr_cd': 'string',
  'da_run_id': 'int32',
  'de_fil_dt': 'object',
  'month': 'int32',
  'elgblty_grp_cd': 'object',
  'benes': 'int64',
  'mdcd_enrlmt': 'int64',
  'chip_enrlmt': 'int64',
  'year': 'object',
  'mdcd_pct_mom': 'float64',
  'chip_pct_mom': 'float64',
  'mdcd_pct_yoy': 'float64',
  'chip_pct_yoy': 'float64',
  'STNAME': 'object',
  'STABBREV': 'object',
  'eligibility_category': 'object'
}
ser = pd.Series(data=d, index=['SUBMTG_STATE_CD','gndr_cd','da_run_id','de_fil_dt','month','elgblty_grp_cd','benes','mdcd_enrlmt','chip_enrlmt','year','mdcd_pct_mom', 'chip_pct_mom', 'mdcd_pct_yoy','chip_pct_yoy','STNAME','STABBREV','eligibility_category'])
ser

# COMMAND ----------

assert (ser == df.dtypes).all(), 'The columns or data types returned do not match the expected values.'
print('DataFrame contains the correct columns.')

# COMMAND ----------

df = enrollapi.byIncomeBracket().fetch()
df.dtypes

# COMMAND ----------

display(df)

# COMMAND ----------

d = {
  'SUBMTG_STATE_CD': 'string',
  'gndr_cd': 'string',
  'incm_cd': 'string',
  'da_run_id': 'int32',
  'de_fil_dt': 'object',
  'month': 'int32',
  'elgblty_grp_cd': 'object',
  'benes': 'int64',
  'mdcd_enrlmt': 'int64',
  'chip_enrlmt': 'int64',
  'year': 'object',
  'mdcd_pct_mom': 'float64',
  'chip_pct_mom': 'float64',
  'mdcd_pct_yoy': 'float64',
  'chip_pct_yoy': 'float64',
  'STNAME': 'object',
  'STABBREV': 'object',
  'eligibility_category': 'object',
  'income': 'object'
}
ser = pd.Series(data=d, index=['SUBMTG_STATE_CD','gndr_cd','incm_cd','da_run_id','de_fil_dt','month','elgblty_grp_cd','benes','mdcd_enrlmt','chip_enrlmt','year','mdcd_pct_mom','chip_pct_mom','mdcd_pct_yoy','chip_pct_yoy','STNAME','STABBREV','eligibility_category','income'])
ser

# COMMAND ----------

assert (ser == df.dtypes).all(), 'The columns or data types returned do not match the expected values.'
print('DataFrame contains the correct columns.')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Time Units

# COMMAND ----------

api = Enrollment().byMonth()

df = api.fetch()

display(df)

# COMMAND ----------

assert (df['counter'] == 'In Month').all(), 'Enrollment object does not return the expected time unit'
print('Enrollment object returns the expected time unit')

# COMMAND ----------

api = Enrollment().byYear()

df = api.fetch()

display(df)

# COMMAND ----------

assert (df['counter'] == 'In Year').all(), 'Enrollment object does not return the expected time unit'
print('Enrollment object returns the expected time unit')

# COMMAND ----------

api = Enrollment()

api.timeunit = 'full'

df = api.fetch()

display(df)

# COMMAND ----------

assert (df['counter'] == 'Full Month').all(), 'Enrollment object does not return the expected time unit'
print('Enrollment object returns the expected time unit')

# COMMAND ----------

api = Enrollment()

api.timeunit = 'partial'

df = api.fetch()

display(df)

# COMMAND ----------

assert (df['counter'] == 'Partial Month').all(), 'Enrollment object does not return the expected time unit'
print('Enrollment object returns the expected time unit')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Context (Object) Changing

# COMMAND ----------

conapi = Enrollment()

assert str(type(conapi)) == "<class 'palet.Enrollment.Enrollment'>", 'The Paletable object is not an Enrollment object.'
print('The Paletable object is an Enrollment object.')

# COMMAND ----------

conapi = Eligibility([6278, 6280], conapi)

assert str(type(conapi)) == "<class 'palet.Eligibility.Eligibility'>", 'The Paletable object is not an Eligibility object.'
print('The Paletable object is an Eligibility object.')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Chronic Condition
# MAGIC Work in progress

# COMMAND ----------

# MAGIC %md
# MAGIC Filtering and marking by one chronic condition

# COMMAND ----------

AFib = ['I230', 'I231', 'I232', 'I233', 'I234', 'I235', 'I236', 'I237', 'I238', 'I213', 'I214', 'I219', 'I220', 'I221', 'I222', 'I228', 'I229', 'I21A1', 'I21A9', 'I2101', 'I2102', 'I2109', 'I2111', 'I2119', 'I2121', 'I2129']

# COMMAND ----------

ccapi = Enrollment([6280]).byMonth().mark(Diagnoses.where(ServiceCategory.inpatient, AFib), 'AFib')

# COMMAND ----------

df = ccapi.fetch()
display(df)

# COMMAND ----------

assert 'AFib' in df.columns, 'Marked chronic conidition is not present in DataFrame'
print('Marked chronic conidition is present in DataFrame')

# COMMAND ----------

Diabetes = [   'E0800','E0801','E0810','E0811','E0821','E0822','E0829','E08311','E08319','E08321','E083211','E083212','E083213','E083219','E08329','E083291','E083292','E083293','E083299','E08331','E083311','E083312','E083313','E083319','E08339','E083391','E083392','E083393','E083399','E08341','E083411','E083412','E083413','E083419','E08349','E083491','E083492','E083493','E083499','E08351','E083511','E083512','E083513','E083519','E083521','E083522','E083523','E083529','E083531','E083532','E083533','E083539','E083541','E083542','E083543','E083549','E083551','E083552','E083553','E083559','E08359','E083591','E083592','E083593','E083599','E0836','E0837X1','E0837X2','E0837X3','E0837X9','E0839','E0840','E0841','E0842','E0843','E0844','E0849','E0851','E0852','E0859','E08610','E08618','E08620','E08621','E08622','E08628','E08630','E08638','E08641','E08649','E0865','E0869','E088','E089','E0900','E0901','E0910','E0911','E0921','E0922','E0929','E09311','E09319','E09321','E093211','E093212','E093213','E093219','E09329','E093291','E093292','E093293','E093299','E09331','E093311','E093312','E093313','E093319','E09339','E093391','E093392','E093393','E093399','E09341','E093411','E093412','E093413','E093419','E09349','E093491','E093492','E093493','E093499','E09351','E093511','E093512','E093513','E093519','E093521','E093522','E093523','E093529','E093531','E093532','E093533','E093539','E093541','E093542','E093543','E093549','E093551','E093552','E093553','E093559','E09359','E093591','E093592','E093593','E093599','E0936','E0937X1','E0937X2','E0937X3','E0937X9','E0939','E0940','E0941','E0942','E0943','E0944','E0949','E0951','E0952','E0959','E09610','E09618','E09620','E09621','E09622','E09628','E09630','E09638','E09641','E09649','E0965','E0969','E098','E099','E1010','E1011','E1021','E1022','E1029','E10311','E10319','E10321','E103211','E103212','E103213','E103219','E10329','E103291','E103292','E103293','E103299','E10331','E103311','E103312','E103313','E103319','E10339','E103391','E103392','E103393','E103399','E10341','E103411','E103412','E103413','E103419','E10349','E103491','E103492','E103493','E103499','E10351','E103511','E103512','E103513','E103519','E10359','E1036','E1037X1','E1037X2','E1037X3','E1037X9','E1039','E1040','E1041','E1042','E1043','E1044','E1049','E1051','E1052','E1059','E10610','E10618','E10620','E10621','E10622','E10628','E10630','E10638','E10641','E10649','E1065','E1069','E108','E109','E1100','E1101','E1110','E1111','E1121','E1122','E1129','E11311','E11319','E11321','E113211','E113212','E113213','E113219','E11329','E113291','E113292','E113293','E113299','E11331','E113311','E113312','E113313','E113319','E11339','E113391','E113392','E113393','E113399','E11341','E113411','E113412','E113413','E113419','E11349','E113491','E113492','E113493','E113499','E11351','E113511','E113512','E113513','E113519','E113521','E113522','E113523','E113529','E113531','E113532','E113533','E113539','E113541','E113542','E113543','E113549','E113551','E113552','E113553','E113559','E11359','E113591','E113592','E113593','E113599','E1136','E1137X1','E1137X2','E1137X3','E1137X9','E1139','E1140','E1141','E1142','E1143','E1144','E1149','E1151','E1152','E1159','E11610','E11618','E11620','E11621','E11622','E11628','E11630','E11638','E11641','E11649','E1165','E1169','E118','E119','E1300','E1301','E1310','E1311','E1321','E1322','E1329','E13311','E13319','E13321','E133211','E133212','E133213','E133219','E13329','E133291','E133292','E133293','E133299','E13331','E133311','E133312','E133313','E133319','E13339','E133391','E133392','E133393','E133399','E13341','E133411','E133412','E133413','E133419','E13349','E133491','E133492','E133493','E133499','E13351','E133511','E133512','E133513','E133519','E133521','E133522','E133523','E133529','E133531','E133532','E133533','E133539','E133541','E133542','E133543','E133549','E133551','E133552','E133553','E133559','E13359','E1336','E1339','E1340','E1341','E1342','E1343','E1344','E1349','E1351','E1352','E1359','E13610','E13618','E13620','E13621','E13622','E13628','E13630','E13638','E13641','E13649','E1365','E1369','E138','E139']

# COMMAND ----------

ccapi = Enrollment([6280]).byMonth()          \
  .mark(                                         \
    Diagnoses.within([                           \
      (ServiceCategory.inpatient, 1),            \
      (ServiceCategory.other_services, 2)],      \
          Diabetes), 'Diabetes')                 \
  .mark(                                         \
     Diagnoses.within([                          \
       (ServiceCategory.inpatient, 1),           \
       (ServiceCategory.other_services, 2)],     \
           AFib), 'AFib')                          

# COMMAND ----------

df = ccapi.fetch()
display(df)

# COMMAND ----------

assert 'AFib' in df.columns and 'Diabetes' in df.columns, 'Marked chronic coniditions are not present in DataFrame'
print('Marked chronic coniditions are present in DataFrame')

# COMMAND ----------

api = Enrollment(period='year').byEnrollmentType()
print(api.sql())
  
df = api.fetch()

# COMMAND ----------

display(df)

# COMMAND ----------

inc = Enrollment()
display(inc.byAgeRange([1,2]).fetch()) 