from palet.Article import Article

class Enrollment(Article):

    #-----------------------------------------------------------------------
    # Initialize the Enrollment API
    #-----------------------------------------------------------------------
    def __init__(self, article:Article =None):
        # print('Initializing Enrollment API')
        super().__init__()

        if (article is not None):
            self.by = article.by
            self.by_group = article.by_group
            self.filter = article.filter
            self.where = article.where
            self.mon_group = article.mon_group


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
        
        rms = self.createView_rid_x_month_x_state()

        new_line_comma = '\n\t\t,'

        z = f"""
            select
                {self.getByGroupWithAlias()}
                2018 as YEAR
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
                YEAR
            order by
                {self.getByGroupWithAlias()}
                YEAR
        """

        return z

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------


# print(Enrollment().byState('37').byEthnicity('01').byAgeRange('18-21').byGender('F').sql())


# print(Enrollment().sql())
# print(Enrollment().byState().sql())
# print(Enrollment().byState('37').sql())
# print(Enrollment().byState('37').byAgeRange('18-21').sql())

# print('-----------------------------------------------------------------------')
# trend = Trend().byState('37').byAgeRange('18-21')
# print(trend.sql())
# print('-----------------------------------------------------------------------')
# enroll = Enrollment(trend)
# print(enroll.sql())
# print('-----------------------------------------------------------------------')
