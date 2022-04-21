"""
The EligibilityType module is a critical component of the by group :meth:`~Paletable.Paletable.byEligibilityType` in :class:`Paletable`.
This module only contains one class, EligibilityType.
"""


# -------------------------------------------------------
#
#
#
# -------------------------------------------------------
class EligibilityType():
    """
    The EligibilityType class creates an alias called enrollment_type that transposes the 12 elgblty_grp_cd
    columns allowing :meth:`~Paletable.Paletable.byEligibilityType` to filter by various eligibility types.
    It also plays a role in the backend method for decorating the eligibility_type_label column
    that is included when one runs :meth:`~Paletable.Paletable.byEligibilityype`
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

    def aggregate(alias):
        a = map(lambda x: alias + '.' + x, EligibilityType.cols)
        b = list(a)
        b.reverse()
        f = ','.join(b)
        return f'coalesce({f})'

    def filter(filter_val):
        a = []
        vals = "','".join(filter_val)
        a.append("('" + vals + "')")

        b = list(a)
        f = ' or '.join(b)
        return f'{f}'

    def __hash__(self):
        return(hash(str(self)))
