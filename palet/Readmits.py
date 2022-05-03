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
    palet_readmits_edge_ip = f"""
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
            da_run_id in ( {  DateDimension().relevant_runids('IPH', 12) } )
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
    palet_readmits_edge_lt = f"""
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
            da_run_id in ( { DateDimension().relevant_runids('LTH', 12) } )
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
    palet_readmits_edge = """
        create or replace temporary view palet_readmits_edge as
        select distinct
             svc_cat
            ,submtg_state_cd
            ,msis_ident_num
            ,blg_prvdr_num
            ,admsn_dt
            ,ptnt_stus_cd
        from (
            select distinct
                 svc_cat
                ,submtg_state_cd
                ,msis_ident_num
                ,blg_prvdr_num
                ,admsn_dt
                ,ptnt_stus_cd
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
                ,ptnt_stus_cd
            from
                palet_readmits_edge_lt
        )
        order by
             svc_cat
            ,submtg_state_cd
            ,msis_ident_num
            ,admsn_dt
            ,blg_prvdr_num
            ,ptnt_stus_cd
        """

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    palet_readmits_edge_x_ip_lt = """
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
            ,e.ptnt_stus_cd
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
            ,e.ptnt_stus_cd
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
    def _sql():
        palet_readmits = f"""
            select
                submtg_state_cd
                ,msis_ident_num
                ,year
                ,month
                ,min(1) as indicator
                ,ptnt_stus_cd
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
                    ,ptnt_stus_cd
                from (
                    select
                        submtg_state_cd
                        ,msis_ident_num
                        ,admit
                        ,max(discharge) as discharge
                        ,ptnt_stus_cd
                    from
                        palet_readmits_edge_x_ip_lt
                    where ptnt_stus_cd not in ('30', null)
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
                )
                order by
                    submtg_state_cd
                    ,msis_ident_num
                    ,admit
                    ,discharge
            )
            where
                lead_diff_days > 1 and lead_diff_days <= { {0} }
            group by
                submtg_state_cd
                ,msis_ident_num
                ,ptnt_stus_cd
                ,year
                ,month
        """
        return palet_readmits

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    @staticmethod
    def allcause(days):

        spark = SparkSession.getActiveSession()
        if spark is not None:

            spark.sql(Readmits.palet_readmits_edge_ip)
            spark.sql(Readmits.palet_readmits_edge_lt)
            spark.sql(Readmits.palet_readmits_edge)
            spark.sql(Readmits.palet_readmits_edge_x_ip_lt)
            # spark.sql(Readmits.palet_readmits)

        palet = Palet.getInstance()
        alias = palet.reserveSQLAlias()

        _snippet = Readmits._sql()
        z = '(' + _snippet.format(str(days)) + ')'

        return f"""{z} as {alias}
            on     aa.submtg_state_cd = {alias}.submtg_state_cd
               and aa.msis_ident_num = {alias}.msis_ident_num
               and aa.de_fil_dt  = {alias}.year
               and month = {alias}.month"""
