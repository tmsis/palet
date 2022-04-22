# -------------------------------------------------------
#
#
#
# -------------------------------------------------------
from pyspark.sql import SparkSession

from palet.Palet import Palet


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
    palet_readmits_edge_ip = """
        create or replace temporary view palet_readmits_edge_ip as
        select distinct
            'IP' as svc_cat
            ,submtg_state_cd
            ,msis_ident_num
            ,admsn_dt
            ,blg_prvdr_num
            ,coalesce(dschrg_dt, srvc_endg_dt_drvd) as dschrg_dt
        from
            taf.taf_iph
        where
                submtg_state_cd = '36'
            and da_run_id in (7204,7205,7206,7207,7208,7209,7210,7211,7212,7213,7214,7215)
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
    palet_readmits_edge_lt = """
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
        from
            taf.taf_lth
        where
            submtg_state_cd = '36'
            and da_run_id in (7216, 7217,7218,7219,7220,7221,7222,7223,7224,7225,7226,7227,7228)
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
    palet_readmits = f"""
        select
             submtg_state_cd
            ,msis_ident_num
            ,year
            ,month
            ,min(1) as indicator
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
                ,year(admit) as year
                ,month(admit) as month
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
        where
            lead_diff_days > 1 and lead_diff_days <= {0}
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
    def allcause(days):

        spark = SparkSession.getActiveSession()
        if spark is not None:

            spark.sql(Readmits.palet_readmits_edge_ip)
            spark.sql(Readmits.palet_readmits_edge_lt)
            spark.sql(Readmits.palet_readmits_edge)
            spark.sql(Readmits.palet_readmits_edge_x_ip_lt)
            spark.sql(Readmits.palet_readmits)

        palet = Palet.getInstance()
        alias = palet.reserveSQLAlias()

        z = '(' + Readmits.palet_readmits.format(days) + ')'

        return f"""{z} as {alias}
            on     aa.submtg_state_cd = {alias}.submtg_state_cd
               and aa.msis_ident_num = {alias}.msis_ident_num
               and aa.de_fil_dt  = {alias}.year
               and month = {alias}.month"""
