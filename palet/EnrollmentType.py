"""
The EnrollmentType module is a critical component of the by group :meth:`~Paletable.Paletable.byEnrollmentType` in :class:`Paletable`.
This module only contains one class, EnrollmentType.
"""


# -------------------------------------------------------
#
#
#
# -------------------------------------------------------
class EnrollmentType():
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

    alias = 'enrollment_type'

    cols = ['chip_cd_01',
            'chip_cd_02',
            'chip_cd_03',
            'chip_cd_04',
            'chip_cd_05',
            'chip_cd_06',
            'chip_cd_07',
            'chip_cd_08',
            'chip_cd_09',
            'chip_cd_10',
            'chip_cd_11',
            'chip_cd_12']

    def aggregate(alias):
        a = map(lambda x: alias + '.' + x, EnrollmentType.cols)
        b = list(a)
        b.reverse()
        f = ','.join(b)
        return f'coalesce({f})'