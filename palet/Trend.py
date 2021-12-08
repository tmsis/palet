from Article import Article
from Palet import Palet
from datetime import date

class Trend(Article):

    ## ----------------------------------------------
    ## Initialization of Trend class
    ## ----------------------------------------------
    def __init__(self):
        print('Initializing Trend API')
        super().__init__()


    ## --------------------------------------------------
    ## getMonthOverMonth function for trends of measures
    ## --------------------------------------------------
    def getMonthOverMonth(self, year=str(date.today().year)):
        self.filter.update({"BSF_FIL_DT":  Palet.Utils.createDateRange(str(year)) })
        self.by_group.append("BSF_FIL_DT")
        self.mon_group.append('mon.BSF_FIL_DT')
        return self





    def mean(col: str):
        print('Calculating Average ' + col)

    def sql(self):

        new_line_comma = '\n\t\t\t   ,'

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

    properties = {

        'age': mean

    }



