"""
The Paletable module contains the Paletable class and its attributes. Said attributes can be combined with top
level objects, such as enrollment, to filter data by a variety of meterics. These metrics include age range,
ethnicity, file data, income bracket, gender and state. Paletable also contains fetch(), the attribute used to
return datafranes created by high level objects.
"""

import pandas as pd

from palet.Palet import Palet
from palet.PaletMetadata import PaletMetadata
from palet.Paletable import Paletable


class Coverage(Paletable):
    """
    Class containing attributes that can be combined with other classes from the PALET library. These
    attributes allow users to filter and return the dataframes created by high level objects.

    Note:
        The Paletable class is inherited from high level objects such as enrollment, as such it does not need to be imported seperately.
        Once imported, the Article class is not called. Rather its attributes are called after a high level object.

    Example:
        Import enrollment:

        >>> from palet.Enrollment import Enrollment

        Create dataframe:

        >>> e = Enrollment().byState()

        Return dataframe:

        >>> e.fetch()

    """

    # -----------------------------------------------------------------------
    # Initialize the Coverage API
    # -----------------------------------------------------------------------
    def __init__(self, paletable: Paletable = None):
        # print('Initializing Enrollment API')
        super().__init__()

        if (paletable is not None):
            self.by_group = paletable.by_group
            self.filter = paletable.filter
            paletable.paletableObjs.append(Coverage)

        self.palet.logger.info('Initializing Coverage API')

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _getValueFromFilter(self, column: str):
        value = self.filter.get(column)  # TODO: required columns handling?
        return column + " = " + value

    # ---------------------------------------------------------------------------------
    #
    # slice and dice here to create the proper sytax for a where clause
    #
    #
    # ---------------------------------------------------------------------------------
    def _defineWhereClause(self):
        clause = ""
        where = []

        if len(self.filter) > 0:
            for key in self.filter:

                # get the value(s) in case there are multiple
                values = self.filter[key]

                # Check for multiple values here, space separator is default
                if str(values).find(" ") > -1:
                    splitRange = self._checkForMultiVarFilter(values)
                    for value in splitRange:
                        clause = ("a." + key, value)
                        where.append(' ((= '.join(clause))

                # Check for multiples with , separator
                elif str(values).find(",") > -1:
                    splitVals = self._checkForMultiVarFilter(values, ",")
                    for values in splitVals:
                        # check for age ranges here with the - separator
                        if str(values).find("-") > -1:
                            splitRange = self._checkForMultiVarFilter(values, "-")
                            range_stmt = "a." + key + " between " + splitRange[0] + " and " + splitRange[1]
                        # check for greater than; i.e. x+ equals >= x
                        elif str(values).find("+") > -1:
                            range_stmt = "a." + key + " >= " + values.strip("+")
                        # take the x+ and strip out the +
                        where.append(range_stmt)

                else:  # else parse the single value
                    clause = ("a." + key, self.filter[key])
                    where.append(' = '.join(clause))

            return f"{' and '.join(where)}"

        else:
            return "1=1"

    # ---------------------------------------------------------------------------------
    #
    # _chcekForMultiVarFilter
    # This is a function that is called to check for multiple value input from the user.
    # It is used internally and called during byGroup calls.
    # ---------------------------------------------------------------------------------
    def _checkForMultiVarFilter(self, values: str, separator=" "):
        return values.split(separator)

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

            self._buildPctChangeColumn(df, 'mdcd_pct_mom', 'mdcd_eligible', 1, False)

            # Year-over-Year
            df = df.sort_values(by=self.by_group + ['month', 'year'], ascending=True)
            df.loc[df.groupby(self.by_group + ['month']).apply(pd.DataFrame.first_valid_index), 'isfirst'] = 1

            self._buildPctChangeColumn(df, 'mdcd_pct_yoy', 'mdcd_eligible', 1, False)

            # Re-sort Chronologically
            df = df.sort_values(by=self.by_group + ['year', 'month'], ascending=True)

        elif self.timeunit == 'year':

            # Year-over-Year
            df = df.sort_values(by=self.by_group + ['year'], ascending=True)
            if (len(self.by_group)) > 0:
                df.loc[df.groupby(self.by_group).apply(pd.DataFrame.first_valid_index), 'isfirst'] = 1
            else:
                df['isfirst'] = 0

            self._buildPctChangeColumn(df, 'mdcd_pct_yoy', 'mdcd_eligible', 1, False)

        return df

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _decorate(self, df):
        self.palet.logger.debug('Decorate')

        # for submitting state
        if PaletMetadata.Enrollment.locale.submittingState in self.by_group:
            df['USPS'] = df['SUBMTG_STATE_CD'].apply(lambda x: str(x).zfill(2))
            df = pd.merge(df, self.palet.st_name,
                          how='inner',
                          left_on=['USPS'],
                          right_on=['USPS'])
            df = pd.merge(df, self.palet.st_usps,
                          how='inner',
                          left_on=['USPS'],
                          right_on=['USPS'])

        return df

    # ---------------------------------------------------------------------------------
    # timeunit class
    # Create the proper summary columns based on the by timeunit selected.
    # Use the stack SQL function to create columns
    #
    # ---------------------------------------------------------------------------------
    class timeunit():

        breakdown = {
            'year': """
                sum(case when a.MC_PLAN_TYPE_CD_ltst > 0 then 1 else 0 end) as mdcd_coverage_type""",
            'month': """
                stack(12,
                    1, sum(case when a.MC_PLAN_TYPE_CD_01 > 0 then 1 else 0 end),
                    2, sum(case when a.MC_PLAN_TYPE_CD_02 > 0 then 1 else 0 end),
                    3, sum(case when a.MC_PLAN_TYPE_CD_03 > 0 then 1 else 0 end),
                    4, sum(case when a.MC_PLAN_TYPE_CD_04 > 0 then 1 else 0 end),
                    5, sum(case when a.MC_PLAN_TYPE_CD_05 > 0 then 1 else 0 end),
                    6, sum(case when a.MC_PLAN_TYPE_CD_06 > 0 then 1 else 0 end),
                    7, sum(case when a.MC_PLAN_TYPE_CD_07 > 0 then 1 else 0 end),
                    8, sum(case when a.MC_PLAN_TYPE_CD_08 > 0 then 1 else 0 end),
                    9, sum(case when a.MC_PLAN_TYPE_CD_09 > 0 then 1 else 0 end),
                    10,sum(case when a.MC_PLAN_TYPE_CD_10 > 0 then 1 else 0 end),
                    11,sum(case when a.MC_PLAN_TYPE_CD_11 > 0 then 1 else 0 end),
                    12,sum(case when a.MC_PLAN_TYPE_CD_12 > 0 then 1 else 0 end)
                ) as (month, mdcd_coverage_type)"""
        }

        cull = {
            'year': """(
                a.MC_PLAN_TYPE_CD_ltst > 0)""",
            'month': """(
                (a.MC_PLAN_TYPE_CD_01 > 0) or
                (a.MC_PLAN_TYPE_CD_02 > 0) or
                (a.MC_PLAN_TYPE_CD_03 > 0) or
                (a.MC_PLAN_TYPE_CD_04 > 0) or
                (a.MC_PLAN_TYPE_CD_05 > 0) or
                (a.MC_PLAN_TYPE_CD_06 > 0) or
                (a.MC_PLAN_TYPE_CD_07 > 0) or
                (a.MC_PLAN_TYPE_CD_08 > 0) or
                (a.MC_PLAN_TYPE_CD_09 > 0) or
                (a.MC_PLAN_TYPE_CD_10 > 0) or
                (a.MC_PLAN_TYPE_CD_11 > 0) or
                (a.MC_PLAN_TYPE_CD_12 > 0)
            )"""
        }

    # ---------------------------------------------------------------------------------
    # _getTimeunitBreakdown
    # Tis function is used to dynamically generate the SQL statement by returning the
    # selected timeunit. e.g. byMonth() or byYear()
    # ---------------------------------------------------------------------------------
    def _getTimeunitBreakdown(self):
        return Coverage.timeunit.breakdown[self.timeunit]

    # ---------------------------------------------------------------------------------
    # _getByTimeunitCull
    # Tis function is used to dynamically generate the SQL where clause by returning the
    # selected timeunit. e.g. byMonth() or byYear()
    # ---------------------------------------------------------------------------------
    def _getByTimeunitCull(self):
        return Coverage.timeunit.cull[self.timeunit]

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
                {self._getTimeunitBreakdown()}
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

        return z

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def fetch(self):
        """Call this method at the end of an object when you are ready for results.

        This can be leveraged with display() to quickly pivot results.

        Args:
            None: No input required

        Returns:
            Spark Datarame: Executes your query and returns a Spark Datarame.

        Example:
            Create object for enrollment by state and year

            >>> api = Enrollment().byState()

            Return Spark DataFrame:

            >>> api.fetch

            Lever display() to pivot from yearly to monthly

            >>> display(api.byMonth().fetch())
        """
        from pyspark.sql import SparkSession
        session = SparkSession.getActiveSession()
        # self.palet.logger.info('Fetching data - \n' + self.sql())

        sparkDF = session.sql(self.sql())
        df = sparkDF.toPandas()

        # perform last minute add-ons here
        for pp in self.postprocesses:
            df = pp(df)

        df = df.drop(columns=['isfirst'])

        return df

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def log(self, viewname: str, sql=''):
        self.palet.logger.info('\t' + viewname)
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
