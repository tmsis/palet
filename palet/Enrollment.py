"""
The Enrollment module allows CMS analysts to view enrollment. This module can be levereged with the Paletable module
to apply specific filters. Doing so, analysts can view enrollment by state, income bracket, age, etc. This module
uses the pandas library and elements of the pyspark library. Note the Paletable module is imported here as well. As such,
the Enrollment module inherits from the Paletable module. 
"""

import pandas as pd
from palet.PaletMetadata import PaletMetadata
from palet.Paletable import Paletable


class Enrollment(Paletable):
    """
    The class within the PALET library for viewing enrollment. This class is used to view enrollment for Medicaid and CHIP.
    can be used to apply and_filters for enrollee age, ehtnicity, gender, state, income, etc.

    Attributes inherited from the Paletable class Enrollment counts are the sum of the unique beneficiaries enrolled at least one day in a given month or year.

    Note:
        If the Enrollment class is called without a by group, it defaults to by year.

    Examples:
        Import enrollment:

        >>> from palet.Enrollment import Enrollment

        Create object for enrollment

        >>> api = Enrollment()

        Return dataframe for yearly enrollment:

        >>> api.fetch()

        Pivot to by state:

        >>> display(api.byState().fetch())

        Pivot to by month and state:

        >>> display(api.byMonth().byState().fetch())

    Args:
        Paletable: No input required, defaults to none

    Returns:
        Spark DataFrame: DataFrame with counts for enrollment and precentage changes from previous period.

    Methods:
        byAgeRange(): Filter your query by Age Range. See :meth:`~Paletable.Paletable.byAgeRange`.
        byRaceEthnicity(): Filter your query by Race. See :meth:`~Paletable.Paletable.byRaceEthnicity`.
        byRaceEthnicityExpanded(): Filter your query by Race (expanded options). See :meth:`~Paletable.Paletable.byRaceEthnicityExpanded`.
        byEthnicity(): Filter your query by Ethnicity. See :meth:`~Paletable.Paletable.byEthnicity`.
        byGender(): Filter your query by Gender. See :meth:`~Paletable.Paletable.byGender`.
        byState(): Filter your query by State. See :meth:`~Paletable.Paletable.byState`.
        byMedicaidOnly(): Filter your query to only look at Medicaid enrollment :meth:`~Paletable.Paletable.byMedicaidOnly`.
        byIncomeBracket(): Filter your query by Income Bracket. See :meth:`~Paletable.Paletable.byIncomeBracket`.
        byYear(): Filter your query by Year. See :meth:`~Paletable.Paletable.byYear`.
        byMonth(): Filter your query by Month. See :meth:`~Paletable.Paletable.byMonth`.
        fetch(): Call this function when you are ready to return results. See :meth:`~Paletable.Paletable.fetch`.

    Note:
        The above attributes are inherited from the :class:`Paletable` class. Attributes directly from the Enrollment class can be seen below.

    """

    # -----------------------------------------------------------------------
    # Initialize the Enrollment API
    # -----------------------------------------------------------------------
    def __init__(self, paletable: Paletable = None):
        # print('Initializing Enrollment API')
        super().__init__()

        if (paletable is not None):
            self.by_group = paletable.by_group
            self.filter = paletable.filter
            paletable.paletableObjs.append(Enrollment)

        self.palet.logger.info('Initializing Enrollment API')

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def medicaid_enrollment_intervals(self):
        print("Start date: " + self.start + " End date: " + self.end)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def chip_enrollment_intervals(self):
        print("Start date: " + self.start + " End date: " + self.end)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def mc_plans(self):
        print('mc_plans')

    # ---------------------------------------------------------------------------------
    # timeunit class
    # Create the proper summary columns based on the by timeunit selected.
    # Use the stack SQL function to create columns
    #
    # ---------------------------------------------------------------------------------
    class timeunit():
        """
        The timeunit class is a subclass within the Enrollment. This subclass provides units of measurement for time.
        It is composed of two dictionaries: breakdown & cull.

        Note:
            This class affects both Medicaid & CHIP Enrollment.
        """

        breakdown = {
            'year': """
                sum(case when a.mdcd_enrlmt_days_yr > 0 then 1 else 0 end) as mdcd_enrollment,
                sum(case when a.chip_enrlmt_days_yr > 0 then 1 else 0 end) as chip_enrollment""",
            'month': """
                stack(12,
                    1, sum(case when a.mdcd_enrlmt_days_01 > 0 then 1 else 0 end),
                       sum(case when a.chip_enrlmt_days_01 > 0 then 1 else 0 end),
                    2, sum(case when a.mdcd_enrlmt_days_02 > 0 then 1 else 0 end),
                       sum(case when a.chip_enrlmt_days_02 > 0 then 1 else 0 end),
                    3, sum(case when a.mdcd_enrlmt_days_03 > 0 then 1 else 0 end),
                       sum(case when a.chip_enrlmt_days_03 > 0 then 1 else 0 end),
                    4, sum(case when a.mdcd_enrlmt_days_04 > 0 then 1 else 0 end),
                       sum(case when a.chip_enrlmt_days_04 > 0 then 1 else 0 end),
                    5, sum(case when a.mdcd_enrlmt_days_05 > 0 then 1 else 0 end),
                       sum(case when a.chip_enrlmt_days_05 > 0 then 1 else 0 end),
                    6, sum(case when a.mdcd_enrlmt_days_06 > 0 then 1 else 0 end),
                       sum(case when a.chip_enrlmt_days_06 > 0 then 1 else 0 end),
                    7, sum(case when a.mdcd_enrlmt_days_07 > 0 then 1 else 0 end),
                       sum(case when a.chip_enrlmt_days_07 > 0 then 1 else 0 end),
                    8, sum(case when a.mdcd_enrlmt_days_08 > 0 then 1 else 0 end),
                       sum(case when a.chip_enrlmt_days_08 > 0 then 1 else 0 end),
                    9, sum(case when a.mdcd_enrlmt_days_09 > 0 then 1 else 0 end),
                       sum(case when a.chip_enrlmt_days_09 > 0 then 1 else 0 end),
                    10,sum(case when a.mdcd_enrlmt_days_10 > 0 then 1 else 0 end),
                       sum(case when a.chip_enrlmt_days_10 > 0 then 1 else 0 end),
                    11,sum(case when a.mdcd_enrlmt_days_11 > 0 then 1 else 0 end),
                       sum(case when a.chip_enrlmt_days_11 > 0 then 1 else 0 end),
                    12,sum(case when a.mdcd_enrlmt_days_12 > 0 then 1 else 0 end),
                       sum(case when a.chip_enrlmt_days_12 > 0 then 1 else 0 end)
                ) as (month, mdcd_enrollment, chip_enrollment)"""
        }

        cull = {
            'year': """(
                (a.mdcd_enrlmt_days_yr > 0) or (a.chip_enrlmt_days_yr > 0))""",
            'month': """(
                (a.mdcd_enrlmt_days_01 > 0) or (a.chip_enrlmt_days_01 > 0) or
                (a.mdcd_enrlmt_days_02 > 0) or (a.chip_enrlmt_days_02 > 0) or
                (a.mdcd_enrlmt_days_03 > 0) or (a.chip_enrlmt_days_03 > 0) or
                (a.mdcd_enrlmt_days_04 > 0) or (a.chip_enrlmt_days_04 > 0) or
                (a.mdcd_enrlmt_days_05 > 0) or (a.chip_enrlmt_days_05 > 0) or
                (a.mdcd_enrlmt_days_06 > 0) or (a.chip_enrlmt_days_06 > 0) or
                (a.mdcd_enrlmt_days_07 > 0) or (a.chip_enrlmt_days_07 > 0) or
                (a.mdcd_enrlmt_days_08 > 0) or (a.chip_enrlmt_days_08 > 0) or
                (a.mdcd_enrlmt_days_09 > 0) or (a.chip_enrlmt_days_09 > 0) or
                (a.mdcd_enrlmt_days_10 > 0) or (a.chip_enrlmt_days_10 > 0) or
                (a.mdcd_enrlmt_days_11 > 0) or (a.chip_enrlmt_days_11 > 0) or
                (a.mdcd_enrlmt_days_12 > 0) or (a.chip_enrlmt_days_12 > 0)
            )"""
        }

    # ---------------------------------------------------------------------------------
    # _percentChange protected/private method that is called by each fetch() call
    # to calculate the % change columns. Each Paletable class should override this
    # and create it's own logic.
    # ---------------------------------------------------------------------------------
    def _percentChange(self, df: pd.DataFrame):
        self.palet.logger.debug('Percent Change')

        df['year'] = df['de_fil_dt']

        if self.timeunit == 'month':

            # Month-over-Month
            df = df.sort_values(by=self.by_group + ['year', 'month'], ascending=True)
            if (len(self.by_group)) > 0:
                df.loc[df.groupby(self.by_group).apply(pd.DataFrame.first_valid_index), 'isfirst'] = 1
            else:
                df['isfirst'] = 0

            self._buildPctChangeColumn(df, 'mdcd_pct_mom', 'mdcd_enrollment', 1, False)
            self._buildPctChangeColumn(df, 'chip_pct_mom', 'chip_enrollment', 1, False)

            # Year-over-Year
            df = df.sort_values(by=self.by_group + ['month', 'year'], ascending=True)
            df.loc[df.groupby(self.by_group + ['month']).apply(pd.DataFrame.first_valid_index), 'isfirst'] = 1

            self._buildPctChangeColumn(df, 'mdcd_pct_yoy', 'mdcd_enrollment', 1, False)
            self._buildPctChangeColumn(df, 'chip_pct_yoy', 'chip_enrollment', 1, False)

            # Re-sort Chronologically
            df = df.sort_values(by=self.by_group + ['year', 'month'], ascending=True)

        elif self.timeunit == 'year':

            # Year-over-Year
            df = df.sort_values(by=self.by_group + ['year'], ascending=True)
            if (len(self.by_group)) > 0:
                df.loc[df.groupby(self.by_group).apply(pd.DataFrame.first_valid_index), 'isfirst'] = 1
            else:
                df['isfirst'] = 0

            self._buildPctChangeColumn(df, 'mdcd_pct_yoy', 'mdcd_enrollment', 1, False)
            self._buildPctChangeColumn(df, 'chip_pct_yoy', 'chip_enrollment', 1, False)

        return df

    # ---------------------------------------------------------------------------------
    # _getTimeunitBreakdown
    # Tis function is used to dynamically generate the SQL statement by returning the
    # selected timeunit. e.g. byMonth() or byYear()
    # ---------------------------------------------------------------------------------
    def _getTimeUnitBreakdown(self):
        return Enrollment.timeunit.breakdown[self.timeunit]

    # ---------------------------------------------------------------------------------
    # _getByTimeunitCull
    # Tis function is used to dynamically generate the SQL where clause by returning the
    # selected timeunit. e.g. byMonth() or byYear()
    # ---------------------------------------------------------------------------------
    def _getByTimeunitCull(self):
        return Enrollment.timeunit.cull[self.timeunit]

    # ---------------------------------------------------------------------------------
    #
    #
    #  SQL Alchemy for Enrollment series by year or year/month for Medicaid and CHIP
    #
    #
    # ---------------------------------------------------------------------------------
    def sql(self):
        """The SQL query that the Enrollment class uses to pull dataframes.

        This can be called allowing an analyst to view the SQL the Enrollment is using.

        Args:
            self: None - no input required.

        Returns:
            str: Returns a text string containing the SQL query run by the Enrollment class.

        Example:
            Create object containing the SQL query:

            >>> q = Enrollment().sql()

            Return the query as text:

            >>> print(q)

            Alternative one line approach:

            >>> print(Enrollment.sql())
        """

        # create or replace temporary view enrollment_by_month as
        z = f"""
            select
                {self._getByGroupWithAlias()}
                a.de_fil_dt,
                {self._getTimeUnitBreakdown()}
            from
                taf.taf_ann_de_base as a
            where
                a.da_run_id in ( {self._getRunIds()} ) and
                {self._getByTimeunitCull()} AND
                {self._defineWhereClause()}
            group by
                {self._getByGroupWithAlias()}
                a.de_fil_dt
            order by
                {self._getByGroupWithAlias()}
                a.de_fil_dt
         """

        self._addPostProcess(self._percentChange)
        self._addPostProcess(self._decorate)

        # compress rows from race ethnicity if it is in the by group
        if (PaletMetadata.Enrollment.raceEthnicity.race in self.by_group): 
            self._addPostProcess(self._buildRaceEthnicityColumn)

        # compress rows from race ethnicity expanded if it is in the by group
        if (PaletMetadata.Enrollment.raceEthnicity.raceExpanded in self.by_group): 
            self._addPostProcess(self._buildRaceEthnicityExpColumn)

        # compress rows from ethnicity if it is in the by group
        if (PaletMetadata.Enrollment.raceEthnicity.ethnicity in self.by_group): 
            self._addPostProcess(self._buildEthnicityColumn)

        # compress rows from submitting state to state if it is in the by group
        if (PaletMetadata.Enrollment.locale.submittingState in self.by_group):
            self._addPostProcess(self._mergeStateEnrollments)

        return z

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
