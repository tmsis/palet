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
class Cost():

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
        self.filter = {}

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
                ,tot_mdcd_pd_amt as total_amount
            from
                taf.taf_iph
            where
                da_run_id in ( {  self.date_dimension.relevant_runids('IPH') } )
                and clm_type_cd in ('{ "','".join(self.clm_type_cds) }')
                and substring(bill_type_cd,3,1) in ('1', '2')
                {{0}}
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
                ,tot_mdcd_pd_amt as total_amount
            from
                taf.taf_lth
            where
                da_run_id in ( { self.date_dimension.relevant_runids('LTH') } )
                and clm_type_cd in ('{ "','".join(self.clm_type_cds) }')
                and substring(bill_type_cd,3,1) in ('1', '2')
                {{0}}
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
                ,total_amount
            from (
                select distinct
                     svc_cat
                    ,submtg_state_cd
                    ,msis_ident_num
                    ,blg_prvdr_num
                    ,admsn_dt
                    ,total_amount
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
                    ,total_amount
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
                ,(coalesce(ip.total_amount,0) + coalesce(lt.total_amount,0)) as total_amount
            from
                palet_admits_edge as e
            left join
                palet_admits_edge_ip as ip
                on      e.msis_ident_num = ip.msis_ident_num
                    and e.blg_prvdr_num = ip.blg_prvdr_num
                    and e.admsn_dt = ip.admsn_dt
            left join
                palet_admits_edge_lt as lt
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
        self.palet_admits_discharge = """
            create or replace temporary view palet_admits_discharge as
            select
                 submtg_state_cd
                ,msis_ident_num
                ,admit
                ,max(discharge) as discharge
                ,ptnt_stus_cd
                ,sum(total_amount) as total_amount
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
                ,total_amount
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
                ,total_amount
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
            select distinct
                submtg_state_cd,
                year(admit) as year,
                month(admit) as month,
                msis_ident_num,
                admit,
                min(1) as units,
                sum(total_amount) as total_amount
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
        self.palet_admits_summary = f"""
            select
                 submtg_state_cd
                ,year
                ,month
                ,sum(units) as units
                ,sum(total_amount) as total_amount
            from
                ({ self.palet_admits })
            group by
                 submtg_state_cd
                ,year
                ,month
            order by
                 submtg_state_cd
                ,year
                ,month
        """

        self.mdcd_mm = f"sum({{parent}}.mdcd_enrollment)"
        self.chip_mm = f"sum({{parent}}.chip_enrollment)"

        self.mdcd_total_amount = f"sum(case when {{parent}}.mdcd_enrollment > 0 then { self.alias }.total_amount else 0 end)"
        self.chip_total_amount = f"sum(case when {{parent}}.chip_enrollment > 0 then { self.alias }.total_amount else 0 end)"

        self.mdcd_pmpm = f"( { self.mdcd_total_amount } / { self.mdcd_mm } )"
        self.chip_pmpm = f"( { self.chip_total_amount } / { self.chip_mm } )"

        self.mdcd_units = f"sum(case when {{parent}}.mdcd_enrollment > 0 then { self.alias }.units else 0 end)"
        self.chip_units = f"sum(case when {{parent}}.chip_enrollment > 0 then { self.alias }.units else 0 end)"

        self.mdcd_util = f"( ( { self.mdcd_units } /  { self.mdcd_mm } ) * 12000 )"
        self.chip_util = f"( ( { self.chip_units } /  { self.chip_mm } ) * 12000 )"

        self.mdcd_cost = f"( { self.mdcd_total_amount } / { self.mdcd_units } )"
        self.chip_cost = f"( { self.chip_total_amount } / { self.chip_units } )"

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def calculate(self):

        # {self.mdcd_mm} as mdcd_mm,
        # {self.chip_mm} as chip_mm,
        # {self.mdcd_units} as mdcd_units,
        # {self.chip_units} as chip_units,

        pmpm = f"""

            round({self.mdcd_total_amount}, 2) as mdcd_total_amount,
            round({self.chip_total_amount}, 2) as chip_total_amount,

            round({self.mdcd_pmpm}, 2) as mdcd_pmpm,
            round({self.chip_pmpm}, 2) as chip_pmpm,

            round({self.mdcd_util}, 1) as mdcd_util,
            round({self.chip_util}, 1) as chip_util,

            round({self.mdcd_cost}, 2) as mdcd_cost,
            round({self.chip_cost}, 2) as chip_cost,

        """
        return pmpm

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def apply_filters(self):
        where = []

        if len(self.filter) > 0:
            for key in self.filter:
                _in_stmt = []
                _join = ""
                if key not in ['SUBMTG_STATE_CD']:
                    continue

                # get the value(s) in case there are multiple
                values = self.filter[key]
                for val in values:
                    _in_stmt.append(f"'{val}'")

                _join = ",".join(_in_stmt)
                where.append(key + ' in (' + _join + ')')

            if len(where) > 0:
                return f"and {' and '.join(where)}"

        else:
            return ''

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def prepare(self):

        self.palet_admits_edge_ip = self.palet_admits_edge_ip.format(self.apply_filters())
        self.palet_admits_edge_lt = self.palet_admits_edge_lt.format(self.apply_filters())

        prep = [
            self.palet_admits_edge_ip,
            self.palet_admits_edge_lt,
            self.palet_admits_edge,
            self.palet_admits_edge_x_ip_lt,
            self.palet_admits_discharge,
            self.palet_admits_segments,
            self.palet_admits_continuity,
            self.palet_admits]

        spark = SparkSession.getActiveSession()
        if spark is not None:
            for i in prep:
                spark.sql(i)

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def join_inner(self) -> str:

        self.prepare()
        
        sql = f"""
                ({self.palet_admits}) as {self.alias}
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

        self.prepare()
        
        sql = f"""
                ({self.palet_admits}) as {self.alias}
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
    def inpatient(date_dimension: DateDimension = None):

        o = Cost()
        if date_dimension is not None:
            o.date_dimension = date_dimension

        palet = Palet.getInstance()
        alias = palet.reserveSQLAlias()
        o.alias = alias
        o.init()

        o.callback = o.calculate

        return o
