"""
The Diagnoses module is a critical component of filtering :class:`Enrollment` by chronic coniditions. This module only contains the :class:`Diagnoses` class and the :meth:`Diagnoses.Diagnoses.where` static method.
"""

# -------------------------------------------------------
#
#
#
# -------------------------------------------------------
from palet.ServiceCategory import ServiceCategory


class Diagnoses:
    """
    The Diagnoses class creates an alias called inpatient that transposes the 12 dgns_cd columns allowing :meth:`Enrollment.Enrollment.having` to filter
    by various coverage types. It also plays a role in the backend method for decorating the chronic conidition column the user specifies.

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

    alias = 'j'

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    @staticmethod
    def _doWhere(service_category: ServiceCategory, diagnoses: list):
        tuples = []
        delim = "','"
        for i in Diagnoses.inpatient:
            tuples.append(f"( {str(i)} in ('{ delim.join(diagnoses) }'))")
        return ' or \n                        '.join(tuples)

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    @staticmethod
    def where(service_category: ServiceCategory, diagnoses: list):
        """
        The static method where() is used to assign parameters for the :meth:`~Enrollment.Enrollment.having` in :class:`Enrollment`. This is where the user assigns
        a service category from the :class:`ServiceCategory` class and the list of diagnoses codes they have specified. 

        Args:
            service_category: `attribute`: Specify an attribute from the :class:`ServiceCategory` such as ServiceCategory.inpaitent
            diagnoses: `list`: User defined list of diagnoses codes

        Returns:
            Spark DataFrame: returns the updated object

        Note:
            The where() function is nested within :meth:`~Enrollment.Enrollment.having`.

        Example:
            Create a list of diagnoses codes:

            >>> AFib = ['I230', 'I231', 'I232', 'I233', 'I234', 'I235', 'I236', 'I237', 'I238', 'I213', 'I214', 'I219', 'I220', 'I221', 'I222', 'I228', 'I229', 'I21A1', 'I21A9', 'I2101', 'I2102', 'I2109', 'I2111', 'I2119', 'I2121', 'I2129']

            Create an Enrollment Object:

            >>> api = Enrollment.ByMonth()

            Use the :meth:`~Enrollment.Enrollment.having` function with where() as a parameter to filter by chronic condition
           
            >>> df = api.having(Diagnoses.where(ServiceCategory.inpatient, AFib))

            Return DataFrame

            >>> display(df.fetch())

        """
        
        return f"""
            (
                select distinct
                    submtg_state_cd,
                    msis_ident_num,
                    1 as indicator
                from
                    taf.data_anltcs_taf_iph_vw
                where
                    da_run_id in (6939, 6938, 6937, 6936, 6935, 6934, 6933, 6932, 6931, 6930, 6929, 6928, 6927)
                    and (
                        { Diagnoses._doWhere(service_category, diagnoses) }
                    )
            ) as {Diagnoses.alias}
                on a.submtg_state_cd = {Diagnoses.alias}.submtg_state_cd and
                   a.msis_ident_num = {Diagnoses.alias}.msis_ident_num
        """
