"""
The Diagnoses module is a critical component of filtering :class:`Enrollment` by chronic coniditions.
This module only contains the :class:`Diagnoses` class and the :meth:`Diagnoses.Diagnoses.where` static method.
"""

# -------------------------------------------------------
#
#
#
# -------------------------------------------------------
from palet.Palet import Palet
from palet.PaletMetadata import PaletMetadata
from palet.ServiceCategory import ServiceCategory


class Diagnoses:
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
                 'dgns_5_cd'
                 ]

    other_services = ['dgns_1_cd',
                      'dgns_2_cd'
                      ]

    link_key = {
        ServiceCategory.inpatient: 'ip_link_key',
        ServiceCategory.other_services: 'ot_link_key',
        ServiceCategory.long_term: 'lt_link_key'
    }

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
                return ' or \n                        '.join(tuples)

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    @staticmethod
    def where(service_category: ServiceCategory, diagnoses: list):
        """
        The static method where() is used to assign parameters for the :meth:`~Enrollment.Enrollment.having` in :class:`Enrollment`.
        This is where the user assigns a service category from the :class:`ServiceCategory` class and the list of diagnoses codes they have specified.

        Args:
            service_category: `attribute`: Specify an attribute from the :class:`ServiceCategory` such as ServiceCategory.inpaitent
            diagnoses: `list`: User defined list of diagnoses codes

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
        palet = Palet.getInstance()
        alias = palet.reserveSQLAlias()

        return f"""
            (
                select distinct
                    submtg_state_cd,
                    msis_ident_num,
                    1 as indicator
                from
                    taf.{ PaletMetadata.Member.service_category.get(service_category) }
                where
                    da_run_id in (6939, 6938, 6937, 6936, 6935, 6934, 6933, 6932, 6931, 6930, 6929, 6928, 6927)
                    and (
                        { Diagnoses._doWhere(service_category, diagnoses) }
                    )
            ) as {alias}
                on aa.submtg_state_cd = {alias}.submtg_state_cd and
                   aa.msis_ident_num = {alias}.msis_ident_num
        """

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    @staticmethod
    def within(service_categories: list, diagnoses: list):

        palet = Palet.getInstance()
        alias = palet.reserveSQLAlias()

        capture = []

        for svc in service_categories:

            service_category = svc[0]

            capture.append(f"""
                select distinct
                    submtg_state_cd,
                    msis_ident_num,
                    min(1) as indicator,
                    count(distinct {Diagnoses.link_key[service_category]}) as m
                from
                    taf.{ PaletMetadata.Member.service_category.get(service_category) }
                where
                    da_run_id in (6939, 6938, 6937, 6936, 6935, 6934, 6933, 6932, 6931, 6930, 6929, 6928, 6927)
                    and (
                        { Diagnoses._doWhere(service_category, diagnoses) }
                    )
                group by
                    submtg_state_cd,
                    msis_ident_num
                having
                    m >= {svc[1]}

            """)

        z = '(' + '\n union all  \n'.join(capture) + ')'

        return f"""{z} as {alias}
            on aa.submtg_state_cd = {alias}.submtg_state_cd and
               aa.msis_ident_num = {alias}.msis_ident_num"""
