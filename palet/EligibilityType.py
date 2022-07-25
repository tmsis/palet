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
    The EligibilityType class creates an alias called eligibility_type that transposes the 12 elgblty_grp_cd
    columns allowing :meth:`~Paletable.Paletable.byEligibilityType` to filter by various eligibility types.
    It also plays a role in the backend method for decorating the eligibility_category column that is included when
    one runs :meth:`~Paletable.Paletable.byEligibilityype` on a Paletable object like :class:`Enrollment`.

    Analysts can filter by an eligibility code such as '01' or one of the predefined attributes below.

    Note:
        PALET users do not need to import this module, it is automatically imported when :meth:`~Paletable.Paletable.byEligibilityType` is called.

    Examples:
        Import Enrollment module:

        >>> from palet.Enrollment import Enrollment

        Create an Enrollment object with an eligibility type by group:

        >>> api = Enrollment().byEligibilityType()

        Convert to a dataframe and return:

        >>> df = api.fetch()

        >>> display(df)

        Filter by a specific eligibility code or multiple codes:

        >>> >>> api = Enrollment().byEligibilityType(['01'])

        >>> >>> api = Enrollment().byEligibilityType(['01','02','03'])

        Convert to a dataframe and return:

        >>> df = api.fetch()

        >>> display(df)

        Filter by a predefined attribute or multiple attributes

        >>> api = Enrollment().byEligibilityType([EligibilityType.BlindAndDisabled])

        >>> api = Enrollment().byEligibilityType([EligibilityType.BlindAndDisabled, EligibilityType.Aged])

        Convert to a dataframe and return:

        >>> df = api.fetch()

        >>> display(df)

    Attributes:

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

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    Pregnant = {"Pregnant": [f"""({{alias}}.age_num <= 59 or {{alias}}.age_num is null) and ({{alias}}.gndr_cd='F' or {{alias}}.gndr_cd is null) and {{alias}}.eligibility_type in ('67','68','05','53')"""]},
    MedicaidChildren = {"MedicaidChildren": [f"""{{alias}}.age_num < 21 and {{alias}}.eligibility_type in ('01','02','03','04','09','14','27','32','33','34','35','36','56','70','71') \
                              or {{alias}}.eligibility_type in ('06','07','08','28','29','30','31','54','55')""", f"""{{alias}}.age_num is null and {{alias}}.eligibility_type in ('54')"""]},
    Adult = {"Adult": [f"""({{alias}}.age_num between 21 and 65 or {{alias}}.age_num is null) and """
                       f"""{{alias}}.eligibility_type in ('01','02','03','04','09','14','27','32','33','34','35','36','56','70','71')""",
                       f"""{{alias}}.age_num is null and {{alias}}.eligibility_type in ('01')"""]},
    BlindAndDisabled = {"BlindAndDisabled": [f"""{{alias}}.age_num < 65 and {{alias}}.eligibility_type in ('11','12','13','15','16','17','18','19','20','22','23','25','26','37','38','39','40','41','42','43','44','46','51','52','59','60')""", f"""{{alias}}.eligibility_type in ('21','24','45','47','48','49','50','69')"""]},
    Aged = {"Aged": [f"""{{alias}}.age_num >= 65 and {{alias}}.eligibility_type in ('01','02','03','04','05','11','12','13','14','15','16','17','18','19','20','22','23','25','26', \
                                                        '27','32','33','34','35','36','37','38','39','40','41','42','43','44','46','51','52','53','56',  \
                                                        '59','60','71')"""]},
    AdultExpansion = {"AdultExpansion": [f"""({{alias}}.age_num >= 18 or {{alias}}.age_num is null) and {{alias}}.eligibility_type in ('72','73','74','75')"""]},
    CHIPChildren = {"CHIPChildren": [f"""({{alias}}.age_num < 21 or {{alias}}.age_num is null) and {{alias}}.eligibility_type in ('61','62','63','64','65','66')"""]},
    COVIDNewlyEligible = {"COVIDNewlyEligible": [f"""({{alias}}.eligibility_type = '76' or {{alias}}.rstrctd_bnfts_cd = 'F') and {{alias}}.de_fil_dt >= '2020' and {{alias}}.month >= '3'"""]}

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def aggregate(alias):
        """
        The aggregate function is responsible for transposing the 12 elgblty_grp_cd's into one column called eligibility_type.
        This column is then used to filter by eligibility type.

        Note:
            This function is not directly interacted with by the user.

        """
        a = map(lambda x: alias + '.' + x, EligibilityType.cols)
        b = list(a)
        b.reverse()
        f = ','.join(b)
        return f'coalesce({f})'

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def filter(self, filter_val):
        """
        The filter function constrains the query by a specific eligibility type or multiple eligibilty types.

        Note:
            This function is not directly interacted with by the user.

        """
        a = []
        filters = []
        if isinstance(filter_val, list) and type(filter_val[0]) is tuple:
            for constraint in filter_val:
                for constr in constraint:
                    for val in constr:
                        strlist = constr[val]
                        for i in strlist:
                            if type(i) is str:
                                filters.append(i)
                            else:
                                for j in i:
                                    filters.append(j)
            self.user_constraint.update({EligibilityType.alias: filters})
        else:
            filters.extend(filter_val)
            self.filter.update({EligibilityType.alias: filters})

        vals = "','".join(filters)
        a.append("('" + vals + "')")

        b = list(a)
        f = ' or '.join(b)
        return f'{f}'

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def __hash__(self):
        return(hash(str(self)))
