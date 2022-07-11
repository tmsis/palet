"""
PALET's Readmits module contains a Readmits class which is a :class:`ClaimsAnalysis` object and can be leveraged with the :class:`Enrollment.Enrollment`
module to look at amount of beneficiary readmissions relative to the ammount of beneficiaries enrolled. A readmission should be viewed as an instance
of a patient who is discharged from a hospital then admitted again within a specific time interval. This time interval can be specified using the
:meth:`~Readmits.Readmits.allcause` method below.
"""

# -------------------------------------------------------
#
#
#
# -------------------------------------------------------
from pyspark.sql import SparkSession

from palet.Palet import Palet
from palet.DateDimension import DateDimension
from palet.ClaimsAnalysis import ClaimsAnalysis


# -------------------------------------------------------
#
#
#
# -------------------------------------------------------
class Readmits(ClaimsAnalysis):
    """
    The Readmits class can be appended to the end of an :class:`Enrollment.Enrollment` object using either :meth:`~Enrollment.Enrollment.having` or :meth:`~Enrollment.Enrollment.calculate`
    from the Enrollment module. In this way, Readmits isn't a Paletable object but a sub-object of Enrollment. As previously mentioned, the :meth:`~Readmits.Readmits.allcause`
    method acts as arguement which allows the user to defined the time interval for readmits as well as the :class:`DateDimension.DateDimension` object associated
    with the Readmits class. Examples of how to use this module are visible below. The Readmits class contains 9 attributes that are SQL queries. These queries are
    essential to the Readmits class as well as the process of joining said query to a Paletable object such as enrollment. Details on these attributes are detailed
    below.

    Note:
        Enrollment().having(Readmits.allcause(30)) filters an enrollment query so counts only include readmits.
        Enrollment().calculate(Readmits.allcause(30)) returns a standard enrollment query and appends columns with counts for readmits as well as a readmit rate.

    Attributes:
        palet_readmits_edge_ip: Pulls information relevant to inpatient claims from taf_iph.
        palet_readmits_edge_lt: Pulls information relevant to long term claims from taf_lth.
        palet_readmits_edge: Unions the data returned from the two initial attributes.
        palet_readmits_edge_x_ip_lt: Pulls data from palet_readmits_edge and joins on values from the initial IP and LT tables.
        palet_readmits_discharge: Adds in logic to account for discharges.
        palet_readmits_segments: Accounts for segmentation.
        palet_readmits_continuity: Accounts for overlapping admits and readmits.
        palet_readmits: The final temporary table joining data from all of the tables above.
        join_sql: The final SQL query which will be joined to the query of the Paletable object.

    Examples:

        Import Enrollment and Readmits from PALET:

        >>> from palet.Enrollment import Enrollment

        >>> from palet.Readmits import Readmits

        Create a Paletable object for Readmits using having()

        >>> having = Enrollment().having(Readmits.allcause(30))

        Convert to a DataFrame and return

        >>> df = having.fetch()

        >>> display(df)

        Create a Paletable object for Readmits using calculate()

        >>> having = Enrollment().calculate(Readmits.allcause(30))

        Convert to a DataFrame and return

        >>> df = having.fetch()

        >>> display(df)

    """

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def __init__(self):
        super().__init__()
        self.days = 30

        self.clm_type_cds = ['1', '3', 'A', 'C', 'U', 'W']

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
        self.join_sql = f"""
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
        """
        The calculate method is not directly interacted with by the analyst. This method is called by :meth:`~Readmits.Readmits.allcause` and responsible for
        computing the columns for readmits, admits and readmit_rate when using :meth:`~Enrollment.Enrollment.calculate` from Enrollment.
        """

        calculate_rate = f"""
            sum({self.alias}.has_readmit) as readmits,
            sum({self.alias}.has_admit) as admits,
            round(sum({self.alias}.has_readmit) / sum({self.alias}.has_admit), 4) as readmit_rate,
        """
        return calculate_rate

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def prepare(self):
        """
        The prepare method is not directly interacted with by the analyst. This method calls :meth:`~ClaimsAnalysis.ClaimsAnalysis.apply_filters`
        from :class:`ClaimsAnalysis` which constrains the initial IP and LT queries from the Attributes section by state if necessary. Additionally,
        this method creates a Spark Session and runs through all of the queries from the attributes section.
        """

        self.palet_readmits_edge_ip = self.palet_readmits_edge_ip.format(self.apply_filters())
        self.palet_readmits_edge_lt = self.palet_readmits_edge_lt.format(self.apply_filters())

        prep = [
            self.palet_readmits_edge_ip,
            self.palet_readmits_edge_lt,
            self.palet_readmits_edge,
            self.palet_readmits_edge_x_ip_lt,
            self.palet_readmits_discharge,
            self.palet_readmits_segments,
            self.palet_readmits_continuity,
            self.palet_readmits]

        spark = SparkSession.getActiveSession()
        if spark is not None:
            for i in prep:
                spark.sql(i)

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    @staticmethod
    def allcause(days, date_dimension: DateDimension = None):
        """
        As previously stated, the allcause function is used define the time interval that determines what would be considered a readmit.
        For example, if the number 15 is entered as an arguement of this function then enrollees admited within 15 days of the initial
        admission would be counted as a readmit. This function is appended onto the end of the Readmit object. More information and
        examples available below.

        Note:
            The number of days defaults to 30, but any number of days may be entered.
            This function also calls the :meth:`~Readmits.Readmits.calculate` method.

        Examples:
            Create a Readmit object with the default time interval:

            >>> readmits = Readmits.allcause()

            Create a Readmit object with a 10 day time interval:

            >>> readmits = Readmits.allcause(10)

            Create a :class:`Paletable` object with a :class:`ClaimsAnalysis` sub-object using :meth:`~Enrollment.Enrollment.having()`

            >>> api = Enrollment().having(Readmits.allcause(30))

            Create a :class:`Paletable` object with a :class:`ClaimsAnalysis` sub-object using :meth:`~Enrollment.Enrollment.calculate()`

            >>> api = Enrollment().calculate(Readmits.allcause(30))

            Convert the object to a DataFrame and return:

            >>> df = api.fetch()

            >>> display(df)

        """

        o = Readmits()
        if date_dimension is not None:
            o.date_dimension = date_dimension

        palet = Palet.getInstance()
        alias = palet.reserveSQLAlias()
        o.alias = alias
        o.init()
        o.days = days

        o.callback = o.calculate

        return o
