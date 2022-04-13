"""
The EnrollmentType module is a critical component of the by group :meth:`~Paletable.Paletable.byEnrollmentType` in :class:`Paletable`.
This module only contains one class, EnrollmentType.
"""


# -------------------------------------------------------
#
#
#
# -------------------------------------------------------
class EligibilityType():
    """
    The EnrollmentType class creates an alias called enrollment_type that transposes the 12 chip_cd
    columns allowing :meth:`~Paletable.Paletable.byEnrollmentType` to filter by various coverage types.
    It also plays a role in the backend method for decorating the enrollment_type_label column
    that is included when one runs :meth:`~Paletable.Paletable.byEnrollmentType`
    on a Paletable object like :class:`Enrollment`.

    Note:
        PALET users and analysts will not interact with this module directly.
        It's purpose is for aliasing and backend decorating that occures when its respective
        by group is called.
    """

    alias = 'eligibility_type'

    cols = ['elgblty_grp_cd_01',
            'elgblty_grp_cd_02',
            'elgblty_grp_cd_03',
            'elgblty_grp_cd_04',
            'elgblty_grp_cd_05',
            'elgblty_grp_cd_06',
            'elgblty_grp_cd_07',
            'elgblty_grp_cd_08',
            'elgblty_grp_cd_09',
            'elgblty_grp_cd_10',
            'elgblty_grp_cd_11',
            'elgblty_grp_cd_12']
