"""
Please note this module is no longer relevant to PALET. It has been consolidated to the by group :meth:`~Paletable.Paletable.byCoverageType` in :class:`Paletable`.

The Coverage module contains the Coverage class. This class is called when an analyst uses the .byCoverageType()
by group. It expands the query being run by the existing object so that filters are applied for various kinds fo coverage.
For example, when this class is utilized along wit enrollment or eligibility, one can view the enrollment counts or
eligibilty counts for each different kind of coverage type.
"""

from Paletable import Paletable


class Coverage(Paletable):
    """
    Class called when an analyst uses the .byCoverageType by group. This can be used to manipulate the query is running
    to break out counts by the specific coverage types. This class is combined with other Paletable objects like enrollment
    eligibility.

    Note:
        The Coverage class does not need to be specifically imported from its respective module. The Coverage class is imported by
        the :meth:`~Paletable.Paletable.byCoverageType` method in :class:`Paletable` when it is called.

    Examples:
        Create an object for Enrollment by month:

        >>> api = Enrollment().byMonth()

        Recreate object with Coverage Type by Group.

        >>> api = api.byCoverageType()

        Return dataframe:

        >>> display(api.fetch())

        User defined run ids:

        >>> api = Enrollment([6278, 6280])

        Specifying run ids and switching context

        >>> api = Coverage([6278, 6280], api) or

        >>> api = Coverage([], api)

        >>> api = Coverage(paletable=api)

    Args:
        list: List of defined run ids you wish to use. Not required, defaults to list of latest run ids.
        Paletable: No input required, defaults to None.

    """

    # -----------------------------------------------------------------------
    # Initialize the Coverage API
    # -----------------------------------------------------------------------
    def __init__(self, runIds: list = None, paletable: Paletable = None):
        # print('Initializing Enrollment API')
        super().__init__(runIds)

        if (paletable is not None):
            self.by_group = paletable.by_group
            self.filter = paletable.filter
            self.derived_by_group = paletable.derived_by_group
            self.defined_columns = paletable.defined_columns
            self._runids = paletable._runids

        self._user_runids = runIds
        self.palet.logger.debug('Initializing Coverage API')

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byType(self, coverage_type=None):
        """Filter your query by Coverage Type. Most top level objects inherit this
        function such as Enrollment, Trend, etc.
        If your object is already set by a by group this will add it as the
        next by group.

        Args:
            coverage_type: `str, optional`: Filter a single coverage tye or multiple coverage types. Defaults to None.

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object
        """

        # self._addByGroup(PaletMetadata.Coverage.mc_plan_type_cd + '01')

        # if coverage_type is not None:
        #     self.filter.update({'MC_PLAN_TYPE_CD_01': coverage_type})

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #  SQL Alchemy for Eligibility series by year or year/month for Medicaid and CHIP
    #
    #
    # ---------------------------------------------------------------------------------
    def sql(self):

        z = f"""
                select
                    {self._getByGroupWithAlias()}
                    a.de_fil_dt,
                    a.month,
                    sum(a.mdcd_enrlmt) as mdcd_enrlmt,
                    sum(a.chip_enrlmt) as chip_enrlmt
                from
                    palet_mart.pivoted_coverage as a
                where
                    coverage_type is not null AND
                    a.da_run_id in ( {self._getRunIds()} )
                group by
                    {self._getByGroupWithAlias()}
                    a.de_fil_dt,
                    a.month
                order by
                    {self._getByGroupWithAlias()}
                    a.de_fil_dt,
                    a.month
            """

        #  TODO: {self._getRunIds()}
        #  TODO: {self._defineWhereClause()}
        #  TODO: {self._getByGroupWithAlias()}

        # compress rows from coverage if it is in the by group
        self._addPostProcess(self._percentChange)

        return z

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def log(self, viewname: str, sql=''):
        """
        This attribute enhances logging. Logging contains multiple levels: INFO, DEBUG, WARNING,
        ERROR and TRACE.
        """
        self.palet.logger.debug('\t' + viewname)
        if sql != '':
            # self.palet.logger.debug(DQPrepETL.compress(sql.replace('\n', '')))
            self.palet.sql[viewname] = '\n'.join(sql.split('\n')[2:])


# -------------------------------------------------------------------------------------
# CC0 1.0 Universal

# Statement of Purpose

# The laws of most jurisdictions throughout the world automatically confer
# exclusive Copyright and Related Rights (defined below) upon the creator and
# subsequent owner(s) (each and all, an "owner") of an original work of
# authorship and/or a database (each, a "Work").

# Certain owners wish to permanently relinquish those rights to a Work for the
# purpose of contributing to a commons of creative, cultural and scientific
# works ("Commons") that the public can reliably and without fear of later
# claims of infringement build upon, modify, incorporate in other works, reuse
# and redistribute as freely as possible in any form whatsoever and for any
# purposes, including without limitation commercial purposes. These owners may
# contribute to the Commons to promote the ideal of a free culture and the
# further production of creative, cultural and scientific works, or to gain
# reputation or greater distribution for their Work in part through the use and
# efforts of others.

# For these and/or other purposes and motivations, and without any expectation
# of additional consideration or compensation, the person associating CC0 with a
# Work (the "Affirmer"), to the extent that he or she is an owner of Copyright
# and Related Rights in the Work, voluntarily elects to apply CC0 to the Work
# and publicly distribute the Work under its terms, with knowledge of his or her
# Copyright and Related Rights in the Work and the meaning and intended legal
# effect of CC0 on those rights.

# 1. Copyright and Related Rights. A Work made available under CC0 may be
# protected by copyright and related or neighboring rights ("Copyright and
# Related Rights"). Copyright and Related Rights include, but are not limited
# to, the following:

#   i. the right to reproduce, adapt, distribute, perform, display, communicate,
#   and translate a Work;

#   ii. moral rights retained by the original author(s) and/or performer(s);

#   iii. publicity and privacy rights pertaining to a person's image or likeness
#   depicted in a Work;

#   iv. rights protecting against unfair competition in regards to a Work,
#   subject to the limitations in paragraph 4(a), below;

#   v. rights protecting the extraction, dissemination, use and reuse of data in
#   a Work;

#   vi. database rights (such as those arising under Directive 96/9/EC of the
#   European Parliament and of the Council of 11 March 1996 on the legal
#   protection of databases, and under any national implementation thereof,
#   including any amended or successor version of such directive); and

#   vii. other similar, equivalent or corresponding rights throughout the world
#   based on applicable law or treaty, and any national implementations thereof.

# 2. Waiver. To the greatest extent permitted by, but not in contravention of,
# applicable law, Affirmer hereby overtly, fully, permanently, irrevocably and
# unconditionally waives, abandons, and surrenders all of Affirmer's Copyright
# and Related Rights and associated claims and causes of action, whether now
# known or unknown (including existing as well as future claims and causes of
# action), in the Work (i) in all territories worldwide, (ii) for the maximum
# duration provided by applicable law or treaty (including future time
# extensions), (iii) in any current or future medium and for any number of
# copies, and (iv) for any purpose whatsoever, including without limitation
# commercial, advertising or promotional purposes (the "Waiver"). Affirmer makes
# the Waiver for the benefit of each member of the public at large and to the
# detriment of Affirmer's heirs and successors, fully intending that such Waiver
# shall not be subject to revocation, rescission, cancellation, termination, or
# any other legal or equitable action to disrupt the quiet enjoyment of the Work
# by the public as contemplated by Affirmer's express Statement of Purpose.

# 3. Public License Fallback. Should any part of the Waiver for any reason be
# judged legally invalid or ineffective under applicable law, then the Waiver
# shall be preserved to the maximum extent permitted taking into account
# Affirmer's express Statement of Purpose. In addition, to the extent the Waiver
# is so judged Affirmer hereby grants to each affected person a royalty-free,
# non transferable, non sublicensable, non exclusive, irrevocable and
# unconditional license to exercise Affirmer's Copyright and Related Rights in
# the Work (i) in all territories worldwide, (ii) for the maximum duration
# provided by applicable law or treaty (including future time extensions), (iii)
# in any current or future medium and for any number of copies, and (iv) for any
# purpose whatsoever, including without limitation commercial, advertising or
# promotional purposes (the "License"). The License shall be deemed effective as
# of the date CC0 was applied by Affirmer to the Work. Should any part of the
# License for any reason be judged legally invalid or ineffective under
# applicable law, such partial invalidity or ineffectiveness shall not
# invalidate the remainder of the License, and in such case Affirmer hereby
# affirms that he or she will not (i) exercise any of his or her remaining
# Copyright and Related Rights in the Work or (ii) assert any associated claims
# and causes of action with respect to the Work, in either case contrary to
# Affirmer's express Statement of Purpose.

# 4. Limitations and Disclaimers.

#   a. No trademark or patent rights held by Affirmer are waived, abandoned,
#   surrendered, licensed or otherwise affected by this document.

#   b. Affirmer offers the Work as-is and makes no representations or warranties
#   of any kind concerning the Work, express, implied, statutory or otherwise,
#   including without limitation warranties of title, merchantability, fitness
#   for a particular purpose, non infringement, or the absence of latent or
#   other defects, accuracy, or the present or absence of errors, whether or not
#   discoverable, all to the greatest extent permissible under applicable law.

#   c. Affirmer disclaims responsibility for clearing rights of other persons
#   that may apply to the Work or any use thereof, including without limitation
#   any person's Copyright and Related Rights in the Work. Further, Affirmer
#   disclaims responsibility for obtaining any necessary consents, permissions
#   or other rights required for any use of the Work.

#   d. Affirmer understands and acknowledges that Creative Commons is not a
#   party to this document and has no duty or obligation with respect to this
#   CC0 or use of the Work.

# For more information, please see
# <http://creativecommons.org/publicdomain/zero/1.0/>
