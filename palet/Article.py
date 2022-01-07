from datetime import datetime
import logging

from pyspark.sql import SparkSession
from palet.Palet import Palet


class Article:

    # TODO: Continue to clean up docstring using syntax formatting
    # Initialize the common variables here.
    # All SQL objects should inherit from this class
    # ----------------------------------------------
    def __init__(self):
        self.by = {}
        self.by_group = []
        self.filter = {}
        self.where = []

        self.mon_group = []

        self.postprocess = []
        self.palet = Palet('201801')
        # This variable exists to save the sql statement from the sub-classes
        self._sql = None

    # ---------------------------------------------------------------------------------
    #   getByGroupWithAlias: This function allows our byGroup to be aliased
    #       properly for the dynamic sql generation
    # ---------------------------------------------------------------------------------
    def _getByGroupWithAlias(self):

        new_line_comma = '\n\t\t\t   ,'
        if (len(self.mon_group)) > 0:
            return f"{new_line_comma.join(self.mon_group)},"
        else:
            return ''

    # Create a temporary table here to optimize our querying of the
    # objects and data
    def _createView_rid_x_month_x_state(self):

        # create or replace temporary view rid_x_month_x_state as
        # TODO: remove the hard coded file data below (2018)
        z = """
            select distinct
                SUBMTG_STATE_CD
                ,BSF_FIL_DT
                ,max(DA_RUN_ID) as DA_RUN_ID
            from
                taf.tmp_max_da_run_id
            where
                BSF_FIL_DT >= 201801 and
                BSF_FIL_DT <= 201812
            group by
                SUBMTG_STATE_CD
                ,BSF_FIL_DT
            order by
                SUBMTG_STATE_CD
                ,BSF_FIL_DT"""
        return z

    # ---------------------------------------------------------------------------------
    # Do last minute add-ons here
    # ---------------------------------------------------------------------------------
    # def _postprocess(self, df: DataFrame, spark: SparkSession):
    #     # import pandas as pd
    #     if self._pctChangeCalc == 1:
    #         pdf = df.toPandas()
    #         pdf.pct_change()
    #         df = spark.createDataFrame(pdf)
    #         return pdf
    #     else:
    #         return self

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
                        clause = ("mon." + key, value)
                        where.append(' ((= '.join(clause))

                # Check for multiples with , separator
                elif str(values).find(",") > -1:
                    splitVals = self._checkForMultiVarFilter(values, ",")
                    for values in splitVals:
                        # check for age ranges here with the - separator
                        if str(values).find("-") > -1:
                            splitRange = self._checkForMultiVarFilter(values, "-")
                            range_stmt = "mon." + key + " between " + splitRange[0] + " and " + splitRange[1]
                        # check for greater than; i.e. x+ equals >= x
                        elif str(values).find("+") > -1:
                            range_stmt = "mon." + key + " >= " + values.strip("+")
                        # take the x+ and strip out the +
                        where.append(range_stmt)

                else:  # else parse the single value
                    clause = ("mon." + key, self.filter[key])
                    where.append(' = '.join(clause))

            return f"where {' and '.join(where)}"

        else:
            return ''

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _checkForMultiVarFilter(self, values: str, separator=" "):
        return values.split(separator)

    # ---------------------------------------------------------------------------------
    def byAgeRange(self, age_range=None):
        """Filter your query by Age Range. Most top level objects inherit this
            function such as Enrollment, Trend, etc.
            If your object is already set by a by group this will add it as the
            next by group.

        Args:
            age_range: `str, optional`: Filter a single age, range such as
            18-21, or an inclusive number such as 65+. Defaults to None.

        Returns:
            :class:`Article` returns the updated object
        """
        self.by_group.append("age_num")
        self.mon_group.append('mon.age_num')

        if age_range is not None:
            self.filter.update({"age_num": age_range})

        return self

    # ---------------------------------------------------------------------------------
    #
    # add any byEthnicity values
    #
    #
    # ---------------------------------------------------------------------------------
    def byEthnicity(self, ethnicity=None):
        """Filter your query by Ethnicity. Most top level objects inherit this function such as Enrollment, Trend, etc.
            If your object is already set by a by group this will add it as the next by group.

        Args:
            ethnicity: `str, optional`: Filter a single ethnicity. Defaults to None.

        Returns:
            :class:`Article`: returns the updated object
        """
        self.by_group.append("race_ethncty_exp_flag")
        self.mon_group.append("mon.race_ethncty_exp_flag")

        if ethnicity is not None:
            self.filter.update({"race_ethncty_exp_flag": "'" + ethnicity + "'"})

        return self

    # ---------------------------------------------------------------------------------
    #
    # add any fileDates here
    # TODO: Figure out the best way to accept dates in this API
    #
    # ---------------------------------------------------------------------------------
    def byFileDate(self, fileDate=None):
        """Filter your query by File Date. Most top level objects inherit this function such as Enrollment, Trend, etc.
            If your object is already set by a by group this will add it as the next by group.

        Args:
            fileDate: `str, optional`: Filter by a file date. Defaults to None.

        Returns:
            :class:`Article` returns the updated object
        """
        self.by_group.append("BSF_FIL_DT")
        self.mon_group.append('mon.BSF_FIL_DT')

        if fileDate is not None:
            self.filter.update({"BSF_FIL_DT": "'" + fileDate + "'"})

        return self

    # ---------------------------------------------------------------------------------
    def byGender(self, gender=None):
        """Filter your query by Gender. Most top level objects inherit this function such as Enrollment, Trend, etc.
            If your object is already set by a by group this will add it as the next by group.

        Args:
            gender: `str, optional`: Filter by gender. Defaults to None.

        Returns:
            :Article Object: returns the updated object
        """
        self.by_group.append("gndr_cd")
        self.mon_group.append('mon.gndr_cd')

        if gender is not None:
            self.filter.update({"gndr_cd": "'" + gender + "'"})

        return self

    # ---------------------------------------------------------------------------------
    def byState(self, state_fips=None):
        """Filter your query by State. Most top level objects inherit this function such as Enrollment, Trend, etc.
            If your object is already set by a by group this will add it as the next by group.

        Args:
            state_fips:`str, (optional)`: Filter by State using FIPS code. See also :func:`State.__init__`. Defaults to None.

        Returns:
            :class:`Article` returns the updated object
        """

        self.palet.logger.info('Group by - state')

        self.by_group.append("SUBMTG_STATE_CD")
        self.mon_group.append('mon.SUBMTG_STATE_CD')

        if state_fips is not None:
            self.filter.update({"SUBMTG_STATE_CD": "'" + state_fips + "'"})

        return self

    # This function is just returning the straight data from the table
    # TODO: If they are looking for analytics calculations we need more details
    def byIncomeBracket(self, bracket=None):
        """Filter your query by income bracket. Most top level objects inherit this function such as Enrollment, Trend, etc.
            If your object is already set by a by group this will add it as the next by group.

        Args
        ----
            bracket:`str, (optional)`: Filter by income. Defaults to None.

        Returns
        -------
            :class:`Article` returns the updated object

        Examples
        --------
        >>> Enrollment.byIncomeBracket('01')
        or
        >>> Trend.byIncomeBracket('01-03')
        or
        >>> Trend.byIncomeBracket('02,03,05')
        """

        self.palet.logger.info('Group by - income bracket')

        self.by_group.append("INCM_CD")
        if bracket is not None:
            self.filter.update({"INCM_CD": "'" + bracket + "'"})
        else:
            self.filter.update({"INCM_CD": "null"})

        return self

    # ---------------------------------------------------------------------------------
    def fetch(self):
        """Call this function when you are ready for results

        Returns:
            :class:`Dataframe`: Executes your query and returns a spark dataframe object.
        """
        session = SparkSession.getActiveSession()
        # self.palet.logger.info('Fetching data - \n' + self.sql())

        sparkDF = session.sql(self._sql())
        df = sparkDF.toPandas()

        # perform last minute add-ons here
        for pp in self.postprocess:
            df = pp(df)

        return df


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
