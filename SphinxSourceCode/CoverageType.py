"""
The CoverageType module is a critical component of the by group :meth:`~Paletable.Paletable.byCoverageType` in :class:`Paletable`. This module only contains one class, CoverageType.
"""

# -------------------------------------------------------
#
#
#
# -------------------------------------------------------
class CoverageType():
    """
    The CoverageType class creates an alias called coverage_type that transposes the 12 mc_plan_type_cd columns allowing :meth:`~Paletable.Paletable.byCoverageType`
    to filter by various coverage types. It also plays a role in the backend method for decorating the coverage_type_label column that is included when one runs :meth:`~Paletable.Paletable.byCoverageType`
    on a Paletable object like :class:`Enrollment`.

    Note:
        PALET users and analysts will not interact with this module directly. It's purpose is for aliasing and backend decorating that occures when its respective
        by group is called. 
    """

    alias = 'coverage_type'

    cols = ['mc_plan_type_cd_01',
            'mc_plan_type_cd_02',
            'mc_plan_type_cd_03',
            'mc_plan_type_cd_04',
            'mc_plan_type_cd_05',
            'mc_plan_type_cd_06',
            'mc_plan_type_cd_07',
            'mc_plan_type_cd_08',
            'mc_plan_type_cd_09',
            'mc_plan_type_cd_10',
            'mc_plan_type_cd_11',
            'mc_plan_type_cd_12']
