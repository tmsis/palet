from Article import Article


class Enrollment(Article):

    #-----------------------------------------------------------------------
    # Initialize the Enrollment API
    #-----------------------------------------------------------------------
    def __init__(self): 
        print('Initializing Enrollment API')
        super().__init__()
        

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    # def __init__(self, start, end):
    #   self.start = start
    #   self.end = end

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def medicaid_enrollment_intervals(self):
        print("Start date: " + self.start + " End date: " + self.end)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def chip_enrollment_intervals(self):
        print("Start date: " + self.start + " End date: " + self.end)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def mc_plans():
        print('mc_plans')

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


    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def sql(self):

        new_line_comma = '\n\t\t,'

        sql = f"""select
            {new_line_comma.join(self.mon_group)}
            , count(*) as m
            from
            taf.taf_mon_bsf as mon
            inner join
            (
                select distinct
                     SUBMTG_STATE_CD
                    ,BSF_FIL_DT
                    ,max(DA_RUN_ID) as DA_RUN_ID
                from
                    taf.tmp_max_da_run_id
                where
                    --BSF_FIL_DT >= 201601 and BSF_FIL_DT <= 201812
                    { self.getValueFromFilter('SUBMTG_STATE_CD')}
                group by
                     SUBMTG_STATE_CD
                    ,BSF_FIL_DT
                order by
                    SUBMTG_STATE_CD

            ) as rid
                on  mon.SUBMTG_STATE_CD = rid.SUBMTG_STATE_CD
                and mon.BSF_FIL_DT = rid.BSF_FIL_DT
                and mon.DA_RUN_ID = rid.DA_RUN_ID

            where
                { self.defineWhereClause() }
            group by
                {new_line_comma.join(self.mon_group)}
            order by
                {new_line_comma.join(self.mon_group)}
        """

        return sql


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


    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------

# E = Enrollment()
# E = E.byState('37').byEthnicity('01').byAgeRange('18-21')
# E.byGender('F')
# E.fetch()

Enrollment().byState('37').byEthnicity('01').byAgeRange('18-21').byGender('F').getSQL()
# e.sql()


# readmit = Readmit(e)


# readmit = Readmit().byState('37').byEthnicity('01').byAgeRange('18-21').byGender('F')

