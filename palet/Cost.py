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
class Cost:

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def __init__(self):
        self.join_sql = ''
        self.callback = None
        self.alias = None
        self.date_dimension = DateDimension.getInstance()

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
        self.palet_admits_edge_ip = f"""
            create or replace temporary view palet_admits_edge_ip as
            select distinct
                'IP' as svc_cat
                ,submtg_state_cd
                ,msis_ident_num
                ,admsn_dt
                ,blg_prvdr_num
                ,coalesce(dschrg_dt, srvc_endg_dt_drvd) as dschrg_dt
                ,ptnt_stus_cd
                ,tot_alowd_amt
            from
                taf.taf_iph
            where
                da_run_id in ( {  self.date_dimension.relevant_runids('IPH') } )
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
        self.palet_admits_edge_lt = f"""
            create or replace temporary view palet_admits_edge_lt as
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
                ,tot_alowd_amt
            from
                taf.taf_lth
            where
                da_run_id in ( { self.date_dimension.relevant_runids('LTH') } )
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
        self.palet_admits_edge = """
            create or replace temporary view palet_admits_edge as
            select distinct
                 svc_cat
                ,submtg_state_cd
                ,msis_ident_num
                ,blg_prvdr_num
                ,admsn_dt
                ,tot_alowd_amt
            from (
                select distinct
                     svc_cat
                    ,submtg_state_cd
                    ,msis_ident_num
                    ,blg_prvdr_num
                    ,admsn_dt
                    ,tot_alowd_amt
                from
                    palet_admits_edge_ip
            )
            union all (
                select distinct
                     svc_cat
                    ,submtg_state_cd
                    ,msis_ident_num
                    ,blg_prvdr_num
                    ,admsn_dt
                    ,tot_alowd_amt
                from
                    palet_admits_edge_lt
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
        self.palet_admits_edge_x_ip_lt = """
            create or replace temporary view palet_admits_edge_x_ip_lt as
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
                ,(coalesce(ip.tot_alowd_amt,0) + coalesce(lt.tot_alowd_amt,0)) as tot_alowd_amt
            from
                palet_admits_edge as e
            left join
                palet_admits_edge_ip as ip
                on      e.msis_ident_num = ip.msis_ident_num
                    and e.blg_prvdr_num = ip.blg_prvdr_num
                    and e.admsn_dt = ip.admsn_dt
            left join
                palet_admits_edge_lt as lt
                on
                        e.msis_ident_num = lt.msis_ident_num
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
        self.palet_admits_discharge = """
            create or replace temporary view palet_admits_discharge as
            select
                 submtg_state_cd
                ,msis_ident_num
                ,admit
                ,max(discharge) as discharge
                ,ptnt_stus_cd
                ,sum(tot_alowd_amt) as tot_alowd_amt
            from
                palet_admits_edge_x_ip_lt
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
        self.palet_admits_segments = """
            create or replace temporary view palet_admits_segments as
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
                ,tot_alowd_amt
            from
                palet_admits_discharge
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
        self.palet_admits_continuity = """
            create or replace temporary view palet_admits_continuity as
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
                    ,tot_alowd_amt
            from
                palet_admits_segments
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
        self.palet_admits = """
            create or replace temporary view palet_admits as
            select
                submtg_state_cd,
                msis_ident_num,
                admit,
                min(1) as units,
                sum(tot_alowd_amt) as tot_alowd_amt
            from
                palet_admits_continuity
            group by
                submtg_state_cd,
                msis_ident_num,
                admit
            order by
                submtg_state_cd,
                msis_ident_num,
                admit
        """

        # -------------------------------------------------------
        #
        #
        #
        # -------------------------------------------------------
        self.palet_admits_summary = """
            select
                submtg_state_cd,
                year(admit) as year,
                month(admit) as month,
                sum(units) as units,
                sum(tot_alowd_amt) as allowed
            from
                palet_admits
            group by
                submtg_state_cd,
                year,
                month
            order by
                submtg_state_cd,
                year,
                month
        """

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def calculate(self):

        pmpm = f"""
            sum({self.alias}.allowed) / sum(bb.mdcd_enrollment) as mdcd_pmpm,
            sum({self.alias}.allowed) / sum(bb.chip_enrollment) as chip_pmpm,
            """

        return pmpm

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    @staticmethod
    def inpatient(date_dimension: DateDimension = None):

        o = Cost()
        if date_dimension is not None:
            o.date_dimension = date_dimension
        o.init()

        spark = SparkSession.getActiveSession()
        if spark is not None:

            spark.sql(o.palet_admits_edge_ip)
            spark.sql(o.palet_admits_edge_lt)
            spark.sql(o.palet_admits_edge)
            spark.sql(o.palet_admits_edge_x_ip_lt)
            spark.sql(o.palet_admits_discharge)
            spark.sql(o.palet_admits_segments)
            spark.sql(o.palet_admits_continuity)
            spark.sql(o.palet_admits)

        palet = Palet.getInstance()
        alias = palet.reserveSQLAlias()

        sql = f"""
            left join
                ({o.palet_admits_summary}) as {alias}
                on      bb.submtg_state_cd = {alias}.submtg_state_cd
                    and bb.de_fil_dt  = {alias}.year
                    and bb.month = {alias}.month"""

        o.join_sql = sql
        o.alias = alias
        o.callback = o.calculate

        return o
