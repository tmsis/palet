from Article import Article
from datetime import date
class Trend(Article):

    ## ----------------------------------------------
    ## Initialization of Trend class
    ## ----------------------------------------------
    def __init__(self, article:Article =None):
        # print('Initializing Trend API')
        super().__init__()

        if (article is not None):
            self.by = article.by
            self.by_group = article.by_group
            self.filter = article.filter
            self.where = article.where
            self.mon_group = article.mon_group


    ## --------------------------------------------------
    ## getMonthOverMonth function for trends of measures
    ## --------------------------------------------------
    def getMonthOverMonth(self, year=str(date.today().year)):
        from Palet import Palet
        
        self.filter.update({"BSF_FIL_DT":  Palet.Utils.createDateRange(str(year)) })
        self.by_group.append("BSF_FIL_DT")
        self.mon_group.append('mon.BSF_FIL_DT')
        return self

    def mean(col: str):
        print('Calculating Average ' + col)


    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def sql(self):

        rms = self.createView_rid_x_month_x_state()

        new_line_comma = '\n\t\t\t   ,'

        z = f"""
                select
                    {self.getByGroupWithAlias()}
                    mon.BSF_FIL_DT
                    , count(*) as m

                from
                    taf.taf_mon_bsf as mon

                inner join
                    ({rms}) as rid    
                        on  mon.SUBMTG_STATE_CD = rid.SUBMTG_STATE_CD    
                        and mon.BSF_FIL_DT = rid.BSF_FIL_DT
                        and mon.DA_RUN_ID = rid.DA_RUN_ID

                {self.defineWhereClause()}

                group by
                   {self.getByGroupWithAlias()}
                    mon.BSF_FIL_DT
                order by
                    {self.getByGroupWithAlias()}
                    mon.BSF_FIL_DT
            """

        return z

# print(Trend().sql())
# print(Trend().byState('37').sql())
# print(Trend().byState('37').byAgeRange('18-21').sql())
