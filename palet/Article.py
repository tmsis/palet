
class Article:

    ## Initialize the common variables here.
    ## All SQL objects should inherit from this class
    ## ----------------------------------------------
    def __init__(self):
        self.by = {}
        self.by_group = []
        self.filter = {}
        self.where = []
        self.mon_group = []

    # ---------------------------------------------------------------------------------
    #   getByGroupWithAlias: This function allows our byGroup to be aliased properly 
    #       for the dynamic sql generation
    # ---------------------------------------------------------------------------------
    def _getByGroupWithAlias(self):
        
        new_line_comma = '\n\t\t\t   ,'
        
        if (len(self.mon_group)) > 0:
            return f"{new_line_comma.join(self.mon_group)},"
        else:
            return ''


    ## Create a temporary table here to optimize our querying of the objects and data
    def _createView_rid_x_month_x_state(self):
        from pyspark.sql import SparkSession

        # create or replace temporary view rid_x_month_x_state as 
        ## TODO: remove the hard coded file data below (2018)
        z = f"""
            select distinct
                SUBMTG_STATE_CD
                ,BSF_FIL_DT
                ,max(DA_RUN_ID) as DA_RUN_ID
            from
                taf.tmp_max_da_run_id
            where
                BSF_FIL_DT >= 201801 and 
                BSF_FIL_DT <= 201812
            group by
                SUBMTG_STATE_CD
                ,BSF_FIL_DT
            order by
                SUBMTG_STATE_CD
                ,BSF_FIL_DT"""

        # spark = SparkSession.getActiveSession()
        # spark.sql(self.sql())
        return z


    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def getValueFromFilter(self, column: str):
        value = self.filter.get(column) ## TODO: what do we do here for required columns
        return column + " = " + value


    # ---------------------------------------------------------------------------------
    #
    # slice and dice here to create the proper sytax for a where clause
    #
    #
    # ---------------------------------------------------------------------------------
    def _defineWhereClause(self):
        clause = ""
        where = []

        if len(self.filter) > 0:
            for key in self.filter:

                # get the value(s) in case there are multiple
                values = self.filter[key]

                if str(values).find(" ") > -1: #Check for multiple values here, space separator is default
                    splitRange = self._checkForMultiVarFilter(values)
                    for value in splitRange:
                        clause = ("mon." + key, value)
                        where.append(' ((= '.join(clause))

                elif str(values).find(",") > -1: #Check for multiples with , separator
                    splitVals = self._checkForMultiVarFilter(values, ",")
                    for values in splitVals:
                        if str(values).find("-") > -1: #check for age ranges here with the - separator
                            splitRange = self._checkForMultiVarFilter(values, "-")
                            range_stmt = "mon." + key + " between " + splitRange[0] + " and " + splitRange[1]
                        elif str(values).find("+") > -1: ## check for greater than; i.e. x+ equals >= x
                            range_stmt = "mon." + key + " >= " + values.strip("+") ## take the x+ and strip out the +
                        where.append(range_stmt)

                else: #else parse the single value
                    clause = ("mon." + key, self.filter[key])
                    where.append(' = '.join(clause))

            return f"where {' and '.join(where)}"

        else:
            return ''

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _checkForMultiVarFilter(self, values: str, separator=" "):
        return values.split(separator)


    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byAgeRange(self, age_range=None):
        self.by_group.append("age_num")
        self.mon_group.append('mon.age_num')
        if age_range != None:
            self.filter.update({"age_num": age_range})
            
        return self       

    ## TODO: Do we want to have this function ONLY for multiple values or do we want to combine this functionality into a range?
    def byAges(self, ages=None):
        self.by_group.append("age_num")
        self.mon_group.append('mon.age_num')
        if ages != None:
            self.filter.update({"age_num": ages})
            
        return self                        

    # ---------------------------------------------------------------------------------
    #
    # add any byEthnicity values
    #
    #
    # ---------------------------------------------------------------------------------
    def byEthnicity(self, ethnicity=None):
        self.by_group.append("race_ethncty_exp_flag")
        self.mon_group.append("mon.race_ethncty_exp_flag")
        if ethnicity != None:
            self.filter.update({"race_ethncty_exp_flag": "'" + ethnicity + "'"})

        return self

    # ---------------------------------------------------------------------------------
    #
    # add any fileDates here
    # TODO: Figure out the best way to accept dates in this API
    #
    # ---------------------------------------------------------------------------------
    def byFileDate(self, fileDate=None):
        self.by_group.append("BSF_FIL_DT")
        self.mon_group.append('mon.BSF_FIL_DT')
        if fileDate != None:
            self.filter.update({"BSF_FIL_DT": "'" + fileDate + "'"})
            
        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byGender(self, gender=None):
        self.by_group.append("gndr_cd")
        self.mon_group.append('mon.gndr_cd')    
        if gender != None:
            self.filter.update({"gndr_cd": "'" + gender + "'"})
            
        return self

    # ---------------------------------------------------------------------------------
    #
    # add any byState values
    #
    #
    # ---------------------------------------------------------------------------------
    def byState(self, state_fips=None):

        self.by_group.append("SUBMTG_STATE_CD")
        self.mon_group.append('mon.SUBMTG_STATE_CD')

        if state_fips != None:
            self.filter.update({"SUBMTG_STATE_CD": "'" + state_fips + "'"})

        return self

    ## This function is just returning the straight data from the table
    ## TODO: If they are looking for analytics calculations we need more details
    def byIncomeBracket(self, bracket=None):
        self.by_group.append("INCM_CD")
        #self.mon_group.append('mon.INCM_CD')

        if bracket != None:
            self.filter.update({"INCM_CD": "'" + bracket + "'"})
        else:
            self.filter.update({"INCM_CD": "null"})

        return self



    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def fetch(self):
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        return spark.sql(self.sql())
