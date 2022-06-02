"""
The Diagnoses module is a critical component of filtering :class:`Enrollment` by chronic coniditions. This module only contains 
the :class:`Diagnoses` class, the :meth:`~Diagnoses.Diagnoses.where` static method and the :meth:`~Diagnoses.Diagnoses.within`.
"""

# -------------------------------------------------------
#
#
#
# -------------------------------------------------------
from numpy import diag
from pyspark.sql import SparkSession

from palet.Palet import Palet
from palet.DateDimension import DateDimension
from palet.PaletMetadata import PaletMetadata
from palet.ServiceCategory import ServiceCategory


# -------------------------------------------------------
#
#
#
# -------------------------------------------------------
class Diagnoses():
    """
    The Diagnoses class creates an alias called inpatient that transposes the 12 dgns_cd columns allowing :meth:`Enrollment.Enrollment.having` to filter
    by various diagnosis types. It also plays a role in the backend method for decorating the chronic conidition column the user specifies.

    """

    inpatient = ['dgns_1_cd',
                 'dgns_2_cd',
                 'dgns_3_cd',
                 'dgns_4_cd',
                 'dgns_5_cd',
                 'dgns_6_cd',
                 'dgns_7_cd',
                 'dgns_8_cd',
                 'dgns_9_cd',
                 'dgns_10_cd',
                 'dgns_11_cd',
                 'dgns_12_cd']

    long_term = ['dgns_1_cd',
                 'dgns_2_cd',
                 'dgns_3_cd',
                 'dgns_4_cd',
                 'dgns_5_cd']

    other_services = ['dgns_1_cd',
                      'dgns_2_cd']

    prescription = ["' 0 '"]

    link_key = {
        ServiceCategory.inpatient: 'ip_link_key',
        ServiceCategory.other_services: 'ot_link_key',
        ServiceCategory.long_term: 'lt_link_key',
        ServiceCategory.prescription: 'rx_link_key'
    }

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def __init__(self):
        self.callback = None
        self.alias = None
        self.date_dimension = DateDimension.getInstance()

        self.service_categories = []
        self.diagnoses = []
        self.within = ''

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def sql(self):
        return self.within

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
        pass

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    @staticmethod
    def _doWhere(service_category: ServiceCategory, diagnoses: list):
        tuples = []
        delim = "','"
        for attr, value in Diagnoses.__dict__.items():
            if attr == service_category:
                for col in value:
                    tuples.append(f"( {str(col)} in ('{ delim.join(diagnoses) }'))")

        return ' or \n\t\t\t'.join(tuples)

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def calculate(self):
        pass
        # return f"{ self.alias }.indicator as ,"

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def join_inner(self) -> str:

        sql = f"""
                ({self.within}) as {self.alias}
                on
                        {{parent}}.{{augment}}submtg_state_cd = {self.alias}.submtg_state_cd
                    and {{parent}}.msis_ident_num = {self.alias}.msis_ident_num"""

        return sql

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def join_outer(self) -> str:

        sql = f"""
                ({self.within}) as {self.alias}
                on
                        {{parent}}.{{augment}}submtg_state_cd = {self.alias}.submtg_state_cd
                    and {{parent}}.msis_ident_num = {self.alias}.msis_ident_num"""

        return sql

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def _getRunIds(run_id_file: ServiceCategory, lookback: int = 6):
        from palet.DateDimension import DateDimension
        return DateDimension.getInstance().relevant_runids(PaletMetadata.Member.run_id_file.get(run_id_file), lookback)


    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    @staticmethod
    def where(service_category: ServiceCategory, diagnoses: list, lookback: int = 6, date_dimension: DateDimension = None):
        """
        The static method where() is used to assign parameters for the :meth:`~Enrollment.Enrollment.having` in :class:`Enrollment`. Unlike the :meth:`~Diagnoses.Diagnoses.within`
        This method is specifically for filtering by a single chronic conditions.
        This is where the user assigns a service category from the :class:`ServiceCategory` class and the list of diagnoses codes they have specified.

        Args:
            service_category: `attribute`: Specify an attribute from the :class:`ServiceCategory` such as ServiceCategory.inpaitent
            diagnoses: `list`: User defined list of diagnoses codes
            lookback: `int defaults to 6`: Enter an integer for the number of years you wish to be included. Defaults to 6.

        Returns:
            Spark DataFrame: returns the updated object

        Note:
            The where() function is nested within :meth:`~Enrollment.Enrollment.having`.

        Example:
            Create a list of diagnoses codes:

            >>> AFib = ['I230', 'I231', 'I232', 'I233', 'I234', 'I235', 'I236', 'I237', 'I238', 'I213', 'I214', 'I219', 'I220',
                        'I221', 'I222', 'I228', 'I229', 'I21A1', 'I21A9', 'I2101', 'I2102', 'I2109', 'I2111', 'I2119', 'I2121', 'I2129']

            Create an Enrollment object & use the :meth:`~Enrollment.Enrollment.having` function with :meth:`~Diagnoses.Diagnoses.where` as a parameter
            to filter by chronic condition:

            >>> api = Enrollment.ByMonth().having(Diagnoses.where(ServiceCategory.inpatient, AFib))

            Return DataFrame:

            >>> display(api.fetch())

            Use the mark function to add a column specifying the chronic condition which the user is filtering by:

            >>> api = Enrollment([6280]).byMonth().mark(Diagnoses.where(ServiceCategory.inpatient, AFib), 'AFib')

            Return the more readable version of the DataFrame:

            >>> display(api.fetch())

        """

        return Diagnoses.within([(service_category, 1)], diagnoses, lookback, date_dimension)


    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    @staticmethod
    def within(service_categories: list, diagnoses: list, lookback: int = 6, date_dimension: DateDimension = None):
        """
        The static method within() is used to assign parameters for the :meth:`~Enrollment.Enrollment.having` in :class:`Enrollment`. Unlike the :meth:`~Diagnoses.Diagnoses.where`
        This method is specifically for filtering by multiple chronic conditions.
        This is where the user assigns a service category from the :class:`ServiceCategory` class and the list of diagnoses codes they have specified.

        Args:
            service_category: `attribute`: Specify an attribute from the :class:`ServiceCategory` such as ServiceCategory.inpaitent
            diagnoses: `list`: User defined list of diagnoses codes
            lookback: `int defaults to 6`: Enter an integer for the number of years you wish to be included. Defaults to 6.

        Returns:
            Spark DataFrame: returns the updated object

        Note:
            The within() function is nested within :meth:`~Enrollment.Enrollment.having`.

        Example:
            Create two lists of diagnoses codes:

            >>> AFib = ['I230', 'I231', 'I232', 'I233', 'I234', 'I235', 'I236', 'I237', 'I238', 'I213', 'I214', 'I219', 'I220',
                        'I221', 'I222', 'I228', 'I229', 'I21A1', 'I21A9', 'I2101', 'I2102', 'I2109', 'I2111', 'I2119', 'I2121', 'I2129']

            >>> Diabetes = [   'E0800','E0801','E0810','E0811','E0821','E0822','E0829','E08311','E08319','E08321','E083211','E083212','E083213',
                        'E083219','E08329','E083291','E083292','E083293','E083299','E08331','E083311','E083312','E083313','E083319','E08339']

            Create an Enrollment object & use the :meth:`~Enrollment.Enrollment.having` function with :meth:`~Diagnoses.Diagnoses.within` as a parameter
            to filter by multiple chronic conditions:

            >>> api = Enrollment([6280]).byMonth().having( \

            >>>     Diagnoses.within([                     \

            >>>     (ServiceCategory.inpatient, 1),        \

            >>>     (ServiceCategory.other_services, 2)],  \

            >>>         Diabetes))

            Return DataFrame:

            >>> display(api.fetch())

            Use the mark function to add a column specifying the chronic condition which the user is filtering by:

            >>> api_mark = Enrollment([6280]).byMonth()          \

            >>>    .mark(                                        \

            >>>        Diagnoses.within([                        \

            >>>        (ServiceCategory.inpatient, 1),           \

            >>>        (ServiceCategory.other_services, 2)],     \

            >>>            Diabetes), 'Diabetes')                \

            >>>    .mark(                                        \

            >>>        Diagnoses.within([                        \

            >>>        (ServiceCategory.inpatient, 1),           \
                
            >>>        (ServiceCategory.other_services, 2)],     \
                
            >>>            AFib), 'AFib')

            Return the more readable version of the DataFrame:

            >>> display(api.fetch())

        """
        o = Diagnoses()
        if date_dimension is not None:
            o.date_dimension = date_dimension

        palet = Palet.getInstance()
        alias = palet.reserveSQLAlias()
        o.alias = alias

        o.service_categories = service_categories
        o.diagnoses = diagnoses

        o.init()

        # -------------------------------------------------------
        #
        # -------------------------------------------------------
        capture = []

        # if type(service_categories) is str:
        #     service_categories = [service_categories]

        for svc in o.service_categories:
            service = svc[0]
            service_num = svc[1]

            capture.append(f"""
                select distinct
                    submtg_state_cd,
                    msis_ident_num,
                    min(1) as indicator,
                    count(distinct {Diagnoses.link_key[service]}) as m
                from
                    taf.{ PaletMetadata.Member.service_category.get(service) }
                where
                    da_run_id in ( { Diagnoses._getRunIds(service, lookback) } )
                    and (
                        { Diagnoses._doWhere(service, o.diagnoses) }
                    )
                group by
                    submtg_state_cd,
                    msis_ident_num
                having
                    m >= { service_num }
            """)
        # -------------------------------------------------------
        o.within = '(' + '\n union all  \n'.join(capture) + ')'

        o.callback = o.calculate

        return o
