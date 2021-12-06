class Article:

  def __init__(self):
    self.by = {}
    self.by_group = []
    self.filter = {}
    self.where = []
    self.mon_group = []


  # ---------------------------------------------------------------------------------
  #
  #
  #
  #
  # ---------------------------------------------------------------------------------
  def getValueFromFilter(self, column: str):
      value = self.filter.get(column)
      return column + " = " + value


  # ---------------------------------------------------------------------------------
  #
  # slice and dice here to create the proper sytax for a where clause
  #
  #
  # ---------------------------------------------------------------------------------
  def defineWhereClause(self):
      clause = ""
      where = []
      for key in self.filter:

          # get the value(s) in case there are multiple
          values = self.filter[key]

          if str(values).find(" ") > -1: #Check for multiple values here, space separator is default
              splitVals = self.checkForMultiVarFilter(values)
              for value in splitVals:
                  clause = ("mon." + key, value)
                  where.append(' = '.join(clause))

          elif str(values).find("-") > -1: #Check for multiples with - separator
              splitVals = self.checkForMultiVarFilter(values, "-")
              range_stmt = "mon." + key + " between " + splitVals[0] + " and " + splitVals[1]

              where.append(range_stmt)

          else: #else parse the single value
              clause = ("mon." + key, self.filter[key])
              where.append(' = '.join(clause))

      return " AND ".join(where)


  # ---------------------------------------------------------------------------------
  #
  #
  #
  #
  # ---------------------------------------------------------------------------------
  def checkForMultiVarFilter(self, values: str, separator=" "):
      return values.split(separator)


  # ---------------------------------------------------------------------------------
  #
  #
  #
  #
  # ---------------------------------------------------------------------------------
  def byAgeRange(self, age_range=None):
      if age_range != None:
          self.filter.update({"age_num": age_range})
          self.by_group.append("age_num")
          self.mon_group.append('mon.age_num')
      return self

  # ---------------------------------------------------------------------------------
  #
  # add any byEthnicity values
  #
  #
  # ---------------------------------------------------------------------------------
  def byEthnicity(self, ethnicity=None):
      if ethnicity != None:
          self.filter.update({"race_ethncty_exp_flag": "'" + ethnicity + "'"})
          self.by_group.append("race_ethncty_exp_flag")
          self.mon_group.append("mon.race_ethncty_exp_flag")
      return self

  # ---------------------------------------------------------------------------------
  #
  # add any fileDates here
  # TODO: Figure out the best way to accept dates in this API
  #
  # ---------------------------------------------------------------------------------
  def byFileDate(self, fileDate=None):
      if fileDate != None:
          self.filter.update({"BSF_FIL_DT": "'" + fileDate + "'"})
          self.by_group.append("BSF_FIL_DT")
          self.mon_group.append('mon.BSF_FIL_DT')
      return self

  # ---------------------------------------------------------------------------------
  #
  #
  #
  #
  # ---------------------------------------------------------------------------------
  def byGender(self, gender=None):
      if gender != None:
          self.filter.update({"gndr_cd": "'" + gender + "'"})
          self.by_group.append("gndr_cd")
          self.mon_group.append('mon.gndr_cd')
      return self

  # ---------------------------------------------------------------------------------
  #
  # add any byState values
  #
  #
  # ---------------------------------------------------------------------------------
  def byState(self, state_fips=None):
      if state_fips != None:
          self.filter.update({"SUBMTG_STATE_CD": "'" + state_fips + "'"})
          self.by_group.append("SUBMTG_STATE_CD")
          self.mon_group.append('mon.SUBMTG_STATE_CD')
      return self