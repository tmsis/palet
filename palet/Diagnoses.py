# -------------------------------------------------------
#
#
#
# -------------------------------------------------------
from palet.ServiceCategory import ServiceCategory


class Diagnoses:

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