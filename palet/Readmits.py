# -------------------------------------------------------
#
#
#
# -------------------------------------------------------
from pyspark.sql import SparkSession

from palet.Palet import Palet
from palet.DateDimension import DateDimension


# -------------------------------------------------------
#
#
#
# -------------------------------------------------------
class Readmits:

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def __init__(self):
        self.days = 30
        self.join_sql = ''
        self.callback = None
        self.alias = None

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def sql(self):
        return self.join_sql

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def __str__(self):
        return self.sql()

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def init(self):

        # -------------------------------------------------------
        #
        #
        #
        # -------------------------------------------------------
        self.palet_readmits_edge_ip = f"""
            create or replace temporary view palet_readmits_edge_ip as
            select distinct
                'IP' as svc_cat
                ,submtg_state_cd
                ,msis_ident_num
                ,admsn_dt
                ,blg_prvdr_num
                ,coalesce(dschrg_dt, srvc_endg_dt_drvd) as dschrg_dt
                ,ptnt_stus_cd
            from
                taf.taf_iph
            where
                da_run_id in ( {  DateDimension().relevant_runids('IPH') } )
                and clm_type_cd in (1, 3)
                and substring(bill_type_cd,3,1) in ('1', '2')
            order by
                msis_ident_num
                ,admsn_dt
                ,blg_prvdr_num
                ,dschrg_dt
        """

        # -------------------------------------------------------
        #
        #
        #
        # -------------------------------------------------------
        self.palet_readmits_edge_lt = f"""
            create or replace temporary view palet_readmits_edge_lt as
            select distinct
                'LT' as svc_cat
                ,submtg_state_cd
                ,msis_ident_num
                ,admsn_dt
                ,blg_prvdr_num
                ,dschrg_dt
                ,srvc_bgnng_dt
                ,srvc_endg_dt
                ,ptnt_stus_cd
            from
                taf.taf_lth
            where
                da_run_id in ( { DateDimension().relevant_runids('LTH') } )
                and clm_type_cd in (1, 3)
                and substring(bill_type_cd,3,1) in ('1', '2')
            order by
                submtg_state_cd
                ,msis_ident_num
                ,blg_prvdr_num
                ,dschrg_dt
                ,srvc_bgnng_dt
                ,srvc_endg_dt
        """

        # -------------------------------------------------------
        #
        #
        #
        # -------------------------------------------------------
        self.palet_readmits_edge = """
            create or replace temporary view palet_readmits_edge as
            select distinct
                svc_cat
                ,submtg_state_cd
                ,msis_ident_num
                ,blg_prvdr_num
                ,admsn_dt
            from (
                select distinct
                    svc_cat
                    ,submtg_state_cd
                    ,msis_ident_num
                    ,blg_prvdr_num
                    ,admsn_dt
                from
                    palet_readmits_edge_ip
            )
            union all (
                select distinct
                    svc_cat
                    ,submtg_state_cd
                    ,msis_ident_num
                    ,blg_prvdr_num
                    ,admsn_dt
                from
                    palet_readmits_edge_lt
            )
            order by
                svc_cat
                ,submtg_state_cd
                ,msis_ident_num
                ,admsn_dt
                ,blg_prvdr_num
            """

        # -------------------------------------------------------
        #
        # palet_readmits_edge_x_ip_lt temporary view
        # uses logic to determine the indiciator
        # -------------------------------------------------------
        self.palet_readmits_edge_x_ip_lt = """
            create or replace temporary view palet_readmits_edge_x_ip_lt as
            select distinct
                e.submtg_state_cd
                ,e.msis_ident_num
                ,e.blg_prvdr_num
                ,case
                    when ((lt.srvc_bgnng_dt <= e.admsn_dt) and (ip.dschrg_dt <= lt.srvc_endg_dt)) then 1
                    when ((e.admsn_dt <= lt.srvc_bgnng_dt) and (lt.srvc_bgnng_dt <= ip.dschrg_dt)) then 1
                    when ((e.admsn_dt <= lt.srvc_endg_dt) and (lt.srvc_endg_dt <= ip.dschrg_dt)) then 1
                    else 0 end as overlap

                ,e.admsn_dt as admit
                ,coalesce(ip.dschrg_dt, lt.dschrg_dt, lt.srvc_endg_dt) as discharge
                ,ip.ptnt_stus_cd
            from
                palet_readmits_edge as e
            left join
                palet_readmits_edge_ip as ip
                on
                        e.msis_ident_num = ip.msis_ident_num
                    and e.blg_prvdr_num = ip.blg_prvdr_num
                    and e.admsn_dt = ip.admsn_dt
            left join
                palet_readmits_edge_lt as lt
                on
                        e.msis_ident_num = lt.msis_ident_num
                    and e.blg_prvdr_num = lt.blg_prvdr_num
                    and e.admsn_dt = lt.admsn_dt
            group by
                e.submtg_state_cd
                ,e.msis_ident_num
                ,e.blg_prvdr_num
                ,lt.srvc_bgnng_dt
                ,lt.srvc_endg_dt
                ,e.admsn_dt
                ,lt.dschrg_dt
                ,ip.dschrg_dt
                ,ip.ptnt_stus_cd
            order by
                e.submtg_state_cd
                ,e.msis_ident_num
                ,admit
                ,discharge
        """

        # -------------------------------------------------------
        #
        #
        #
        # -------------------------------------------------------
        self.palet_readmits_t = f"""
            create or replace temporary view palet_readmits_t as
            select
                submtg_state_cd
                ,msis_ident_num
                ,year
                ,month
                ,count(distinct msis_ident_num) as is_admit
                ,sum(lead_diff_days) as m
                ,case when (min(lead_diff_days) > 1 and min(lead_diff_days) <= {self.days}) then count(distinct msis_ident_num) else 0 end as is_readmit
            from (
                select
                    submtg_state_cd
                    ,msis_ident_num
                    ,admit
                    ,discharge
                    ,datediff(lead(admit) over (
                        partition by
                            submtg_state_cd
                            ,msis_ident_num
                        order by
                            submtg_state_cd
                            ,msis_ident_num
                    ), discharge) as lead_diff_days
                    ,year(discharge) as year
                    ,month(discharge) as month
                from (
                    select
                        submtg_state_cd
                        ,msis_ident_num
                        ,admit
                        ,max(discharge) as discharge
                    from
                        palet_readmits_edge_x_ip_lt
                    group by
                        submtg_state_cd
                        ,msis_ident_num
                        ,admit
                    order by
                        submtg_state_cd
                        ,msis_ident_num
                        ,admit
                        ,discharge
                )
                order by
                    submtg_state_cd
                    ,msis_ident_num
                    ,admit
                    ,discharge
            )
            group by
                submtg_state_cd
                ,msis_ident_num
                ,year
                ,month
        """

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def calculate_rate(self):
        calculate_rate = f"""
        sum({self.alias}.is_admit) as admits,
        sum({self.alias}.is_readmit) as readmits,
        sum({self.alias}.is_readmit) / sum({self.alias}.is_admit) as readmit_rate,
        """
        return calculate_rate

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    @staticmethod
    def allcause(days):

        o = Readmits()
        o.init()
        o.days = days

        spark = SparkSession.getActiveSession()
        if spark is not None:

            spark.sql(o.palet_readmits_edge_ip)
            spark.sql(o.palet_readmits_edge_lt)
            spark.sql(o.palet_readmits_edge)
            spark.sql(o.palet_readmits_edge_x_ip_lt)
            spark.sql(o.palet_readmits_t)

        palet = Palet.getInstance()
        alias = palet.reserveSQLAlias()

        z = """(
                select
                    submtg_state_cd
                    ,year
                    ,month
                    ,sum(is_admit) as is_admit
                    ,sum(is_readmit) as is_readmit
                from
                    palet_readmits_t
                group by
                    submtg_state_cd
                    ,year
                    ,month
                order by
                    submtg_state_cd
                    ,year
                    ,month
                )
            """

        # sql = f"""{z} as {alias}
        #     on     aa.submtg_state_cd = {alias}.submtg_state_cd
        #        and aa.msis_ident_num = {alias}.msis_ident_num
        #        and aa.de_fil_dt  = {alias}.year
        #        and month = {alias}.month"""

        sql = f"""
            left join
                {z} as {alias}
                on      bb.submtg_state_cd = {alias}.submtg_state_cd
                    and bb.de_fil_dt  = {alias}.year
                    and bb.month = {alias}.month"""

        o.join_sql = sql
        o.alias = alias
        o.callback = o.calculate_rate

        return o
