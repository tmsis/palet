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

# e_sql = Enrollment().byState('37').byEthnicity('01').byAgeRange('18-21').byGender('F').sql()
# print(e_sql)




# e.sql()
# readmit = Readmit(e)
# readmit = Readmit().byState('37').byEthnicity('01').byAgeRange('18-21').byGender('F')

