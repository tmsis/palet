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
class Readmits():

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
        self.date_dimension = DateDimension.getInstance()

        self.clm_type_cds = ['1', '3', 'A', 'C', 'U', 'W']

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
                da_run_id in ( {  self.date_dimension.relevant_runids('IPH') } )
                and clm_type_cd in ('{ "','".join(self.clm_type_cds) }')
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
                da_run_id in ( { self.date_dimension.relevant_runids('LTH') } )
                and clm_type_cd in ('{ "','".join(self.clm_type_cds) }')
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
        #
        #
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
                ,coalesce(ip.ptnt_stus_cd, lt.ptnt_stus_cd) as ptnt_stus_cd
            from
                palet_readmits_edge as e
            left join
                palet_readmits_edge_ip as ip
                on      e.msis_ident_num = ip.msis_ident_num
                    and e.blg_prvdr_num = ip.blg_prvdr_num
                    and e.admsn_dt = ip.admsn_dt
            left join
                palet_readmits_edge_lt as lt
                on      e.msis_ident_num = lt.msis_ident_num
                    and e.blg_prvdr_num = lt.blg_prvdr_num
                    and e.admsn_dt = lt.admsn_dt
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
        self.palet_readmits_discharge = """
            create or replace temporary view palet_readmits_discharge as
            select
                 submtg_state_cd
                ,msis_ident_num
                ,admit
                ,max(discharge) as discharge
                ,ptnt_stus_cd
            from
                palet_readmits_edge_x_ip_lt
            group by
                 submtg_state_cd
                ,msis_ident_num
                ,admit
                ,ptnt_stus_cd
            order by
                 submtg_state_cd
                ,msis_ident_num
                ,admit
                ,discharge
        """

        # -------------------------------------------------------
        #
        #
        #
        # -------------------------------------------------------
        self.palet_readmits_segments = """
            create or replace temporary view palet_readmits_segments as
            select
                 submtg_state_cd
                ,msis_ident_num
                ,case
                    when datediff(lead(admit) over (
                        partition by
                             submtg_state_cd
                            ,msis_ident_num
                        order by
                             submtg_state_cd
                            ,msis_ident_num
                        ), discharge) = 1 and ptnt_stus_cd = 30 then 1
                    when datediff(admit, lag(discharge) over (
                        partition by
                             submtg_state_cd
                            ,msis_ident_num
                        order by
                             submtg_state_cd
                            ,msis_ident_num
                        )) = 1 and ptnt_stus_cd = 30 then 1
                    when datediff(lead(admit) over (
                        partition by
                             submtg_state_cd
                            ,msis_ident_num
                        order by
                             submtg_state_cd
                            ,msis_ident_num
                        ), discharge) = 0 then 1
                    when datediff(admit, lag(discharge) over (
                        partition by
                             submtg_state_cd
                            ,msis_ident_num
                        order by
                            submtg_state_cd
                            ,msis_ident_num
                        )) = 0 then 1
                    else 0 end as carry
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
                ,ptnt_stus_cd
            from
                palet_readmits_discharge
            order by
                 submtg_state_cd
                ,msis_ident_num
                ,admit
                ,discharge
        """

        # -------------------------------------------------------
        #
        #
        #
        # -------------------------------------------------------
        self.palet_readmits_continuity = """
            create or replace temporary view palet_readmits_continuity as
            select distinct
                 submtg_state_cd
                ,msis_ident_num
                ,case when carry = 1 then min(admit) over (
                    partition by
                         submtg_state_cd
                        ,msis_ident_num
                        ,carry
                    order by
                         submtg_state_cd
                        ,msis_ident_num
                        ,admit)
                    else admit end as admit
                ,case when carry = 1 then max(discharge) over (
                    partition by
                         submtg_state_cd
                        ,msis_ident_num
                        ,carry
                    order by
                         submtg_state_cd
                        ,msis_ident_num
                        ,discharge desc)
                    else discharge end as discharge
            from
                palet_readmits_segments
            order by
                 submtg_state_cd
                ,msis_ident_num
                ,admit
                ,discharge
        """

        # -------------------------------------------------------
        #
        #
        #
        # -------------------------------------------------------
        self.palet_readmits = f"""
            create or replace temporary view palet_readmits as
            select distinct
                 submtg_state_cd
                ,msis_ident_num
                ,year(admit) as year
                ,month(admit) as month
                ,1 as admit_ind
                ,case when (datediff(lead(admit) over (
                    partition by
                        submtg_state_cd
                        ,msis_ident_num
                    order by
                        submtg_state_cd
                        ,msis_ident_num
                ), discharge) <= {self.days}) then 1 else 0 end as readmit_ind
            from
                palet_readmits_continuity
            order by
                submtg_state_cd,
                msis_ident_num,
                year,
                month
        """

        # -------------------------------------------------------
        #
        #
        #
        # -------------------------------------------------------
        self.palet_readmits_summary = f"""
            select
                 submtg_state_cd
                ,year
                ,month
                ,msis_ident_num
                ,sum(readmit_ind) as has_readmit
                ,sum(admit_ind) as has_admit
                ,sum(readmit_ind) / sum(admit_ind) as readmit_rate
            from
                palet_readmits
            group by
                 submtg_state_cd
                ,year
                ,month
                ,msis_ident_num
            order by
                 submtg_state_cd
                ,year
                ,month
        """

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def calculate(self):

        calculate_rate = f"""
            sum({self.alias}.has_readmit) as readmits,
            sum({self.alias}.has_admit) as admits,
            sum({self.alias}.has_readmit) / sum({self.alias}.has_admit) as readmit_rate,
        """
        return calculate_rate

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def join_inner(self) -> str:

        sql = f"""
                ({self.palet_readmits_summary}) as {self.alias}
                on
                        {{parent}}.{{augment}}submtg_state_cd = {self.alias}.submtg_state_cd
                    and {{parent}}.msis_ident_num = {self.alias}.msis_ident_num
                    and {{parent}}.de_fil_dt = {self.alias}.year"""

        return sql

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def join_outer(self) -> str:

        sql = f"""
                ({self.palet_readmits_summary}) as {self.alias}
                on
                        {{parent}}.{{augment}}submtg_state_cd = {self.alias}.submtg_state_cd
                    and {{parent}}.msis_ident_num = {self.alias}.msis_ident_num
                    and {{parent}}.de_fil_dt = {self.alias}.year
                    and {{parent}}.month = {self.alias}.month"""

        return sql

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    @staticmethod
    def allcause(days, date_dimension: DateDimension = None):

        o = Readmits()
        if date_dimension is not None:
            o.date_dimension = date_dimension

        palet = Palet.getInstance()
        alias = palet.reserveSQLAlias()
        o.alias = alias
        o.init()
        o.days = days

        spark = SparkSession.getActiveSession()
        if spark is not None:

            spark.sql(o.palet_readmits_edge_ip)
            spark.sql(o.palet_readmits_edge_lt)
            spark.sql(o.palet_readmits_edge)
            spark.sql(o.palet_readmits_edge_x_ip_lt)
            spark.sql(o.palet_readmits_discharge)
            spark.sql(o.palet_readmits_segments)
            spark.sql(o.palet_readmits_continuity)
            spark.sql(o.palet_readmits)

        o.callback = o.calculate

        return o
