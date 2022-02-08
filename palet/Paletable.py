"""
The Paletable module contains the Paletable class and its attributes. Said attributes can be combined with top
level objects, such as enrollment, to filter data by a variety of meterics. These metrics include age range,
ethnicity, file data, income bracket, gender and state. Paletable also contains fetch(), the attribute used to
return datafranes created by high level objects.
"""

import pandas as pd

from palet.Palet import Palet
from palet.PaletMetadata import PaletMetadata


class Paletable:
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

    # TODO: Continue to clean up docstring using syntax formatting
    # Initialize the comann variables here.
    # All SQL objects should inherit from this class
    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self):

        self.timeunit = 'year'
        self.by_group = []
        self.filter = {}
        self.lookback = 1

        self.preprocesses = []
        self.postprocesses = []
        self.paletableObjs = []

        self.palet = Palet('201801')

        self.runids = self.palet.cache_run_ids()

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _getRunIds(self):
        return ','.join(map(str, self.runids))

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _addPreProcess(self, cb):
        if cb not in self.preprocesses:
            self.palet.logger.debug(f'Registering Pre Process {cb}')
            self.preprocesses.append(cb)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _addPostProcess(self, cb):
        if cb not in self.postprocesses:
            self.palet.logger.debug(f'Registering Post Process {cb}')
            self.postprocesses.append(cb)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _addByGroup(self, var):
        if var not in self.by_group:
            self.palet.logger.debug(f'Adding By Group {var}')
            self.by_group.append(var)

    # ---------------------------------------------------------------------------------
    #
    #   getByGroupWithAlias: This function allows our byGroup to be aliased
    #       properly for the dynamic sql generation
    #
    # ---------------------------------------------------------------------------------
    def _getByGroupWithAlias(self):
        z = ""
        new_line_comma = '\n\t\t\t   ,'
        if (len(self.by_group)) > 0:
            for column in self.by_group:
                z += "a." + column + new_line_comma
            return f"{z}"
        else:
            return ''

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
    #  _buildPctChangeColumn
    #  This function is used interally to create the pctChange
    #  columns based on the field names to be calculated
    #
    #
    # ---------------------------------------------------------------------------------
    def _buildPctChangeColumn(self, df: pd.DataFrame, resultColumnName: str, columnNameToCalc: str, colIntPosition, isPct: bool):

        df[resultColumnName] = [
            round(((df[columnNameToCalc].iat[x] / df[columnNameToCalc].iat[x-colIntPosition]) - 1), 3)
            if x != 0 and df[columnNameToCalc].iat[x-1] > 0 and df['isfirst'].iat[x] != 1
            else float('NaN')
            for x in range(len(df))]

    # --------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _mergeStateEnrollments(self, df: pd.DataFrame):
        self.palet.logger.debug('Merging separate state enrollments')
        timeunit = 'month'

        if 'year' in df.columns:
            timeunit = 'year'

        df.drop(['USPS', 'SUBMTG_STATE_CD', 'isfirst'], axis=1)
        df.groupby(by=['STABBREV', 'de_fil_dt', timeunit]).sum().reset_index()

        return df

    # --------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _findRaceValueName(self, x):
        import math
        # get this row's ref value from the column by name
        y = x['race_ethncty_flag']
        # lookup label with value
        return PaletMetadata.Enrollment.raceEthnicity.race_ethncty_flag.get(y)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _buildRaceEthnicityColumn(self, df: pd.DataFrame):
        df['race'] = df.apply(lambda x: self._findRaceValueName(x), axis=1)

        return df

    # --------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _findRaceExpValueName(self, x):
        import math
        # get this row's ref value from the column by name
        y = x['race_ethncty_exp_flag']
        # lookup label with value
        return PaletMetadata.Enrollment.raceEthnicity.race_ethncty_exp_flag.get(y)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _buildRaceEthnicityExpColumn(self, df: pd.DataFrame):
        df['raceExpanded'] = df.apply(lambda x: self._findRaceExpValueName(x), axis=1)

        return df

    # --------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _findEthnicityValueName(self, x):
        # get this row's ref value from the column by name
        y = x[PaletMetadata.Enrollment.raceEthnicity.ethnicity]
        # lookup label with value
        return PaletMetadata.Enrollment.raceEthnicity.ethncty_cd.get(y)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _buildEthnicityColumn(self, df: pd.DataFrame):
        df['ethnicity'] = df.apply(lambda x: self._findEthnicityValueName(x), axis=1)

        return df

    # --------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _findValueName_(self, x):
        # get this row's ref value from the column by name
        print(x)
        y = x[PaletMetadata.Coverage.md_plan_type]
        # if the value is NaN, default to unknown
        print(y)
        if y is None or y == 'null':
            return 'unknown'
        else:
            # lookup label with value
            return PaletMetadata.Coverage.md_plan_type.get(y)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _buildValueColumn(self, df: pd.DataFrame):
        newColumn = "coverage_type"
        df[newColumn] = df.apply(lambda x: self._findValueName_(x), axis=1)

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
    #
    #
    #
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
            Spark DataFrame: :class:`Paletable`: returns the updated object
        """

        self._addByGroup(PaletMetadata.Enrollment.identity.age)

        if age_range is not None:
            self.filter.update({PaletMetadata.Enrollment.identity.age: age_range})

        return self

    # ---------------------------------------------------------------------------------
    #
    # add any byEthnicity values
    #
    #
    # ---------------------------------------------------------------------------------
    def byRaceEthnicity(self, ethnicity=None):
        """Filter your query by Race. Most top level objects inherit this function such as Enrollment, Trend, etc.
            If your object is already set by a by group this will add it as the next by group.

        Args:
            ethnicity: `str, optional`: Filter a single ethnicity. Defaults to None.

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object
        """

        self._addByGroup(PaletMetadata.Enrollment.raceEthnicity.race)

        if ethnicity is not None:
            self.filter.update({PaletMetadata.Enrollment.raceEthnicity.race: "'" + ethnicity + "'"})

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byRaceEthnicityExpanded(self, ethnicity=None):
        """Filter your query by Expanded Race. Most top level objects inherit this function such as Enrollment, Trend, etc.
            If your object is already set by a by group this will add it as the next by group.

        Args:
            ethnicity: `str, optional`: Filter a single expanded ethnicity group. Defaults to None.

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object
        """
        self._addByGroup(PaletMetadata.Enrollment.raceEthnicity.raceExpanded)

        if ethnicity is not None:
            self.filter.update({PaletMetadata.Enrollment.raceEthnicity.raceExpanded: "'" + ethnicity + "'"})

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byEthnicity(self, ethnicity=None):
        """Filter your query by Ethnicity. Most top level objects inherit this function such as Enrollment, Trend, etc.
            If your object is already set by a by group this will add it as the next by group.

        Args:
            ethnicity: `str, optional`: Filter a single expanded ethnicity group. Defaults to None.

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object
        """
        self._addByGroup(PaletMetadata.Enrollment.raceEthnicity.ethnicity)

        if ethnicity is not None:
            self.filter.update({PaletMetadata.Enrollment.raceEthnicity.ethnicity: "'" + ethnicity + "'"})

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byGender(self, gender=None):
        """Filter your query by Gender. Most top level objects inherit this function such as Enrollment, Trend, etc.
            If your object is already set by a by group this will add it as the next by group.

        Args:
            gender: `str, optional`: Filter by gender. Defaults to None.

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object
        """

        self.palet.logger.info('Group by - gender')

        self._addByGroup(PaletMetadata.Enrollment.identity.gender)

        if gender is not None:
            self.filter.update({PaletMetadata.Enrollment.identity.gender: "'" + gender + "'"})

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byState(self, state_cd=None):
        """Filter your query by State with total enrollment. Most top level objects inherit this function such as Enrollment, Trend, etc.
            If your object is already set by a by group this will add it as the next by group.

        Args:
            state_fips:`str, (optional)`: Filter by State using FIPS code. See also :func:`State.__init__`. Defaults to None.

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object
        """

        self.palet.logger.info('Group by - state')

        self._addByGroup(PaletMetadata.Enrollment.locale.submittingState)

        if state_cd is not None:
            if type(state_cd) is not int:
                lkup = self.palet.st_fips
                fips = lkup[lkup['STABBREV'] == state_cd]['FIPS']
                state_fips = fips.iloc[0]
            else:
                state_fips = state_cd
            self.filter.update({PaletMetadata.Enrollment.locale.submittingState: "'" + state_fips + "'"})

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byMedicaidOnly(self, state_fips=None):
        """Filter your query by State. Most top level objects inherit this function such as Enrollment, Trend, etc.
            If your object is already set by a by group this will add it as the next by group.

        Args:
            state_fips:`str, (optional)`: Filter by State using FIPS code. See also :func:`State.__init__`. Defaults to None.

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object
        """

        self.palet.logger.info('Group by - state')

        self._addByGroup(PaletMetadata.Enrollment.locale.submittingState)

        if state_fips is not None:
            self.filter.update({PaletMetadata.Enrollment.locale.submittingState: "'" + state_fips + "'"})

        for month in PaletMetadata.Enrollment.CHIP.half1:
            for field in month:
                if field in self.filter:
                    del self.filter[field]
                    del self.by_group[field]
            for month in PaletMetadata.Enrollment.CHIP.half2:
                for field in month:
                    if field in self.filter:
                        del self.filter[field]
                        del self.by_group[field]
        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    # This function is just returning the straight data from the table
    def byIncomeBracket(self, bracket=None):
        """Filter your query by income bracket. Most top level objects inherit this function such as Enrollment, Trend, etc.
            If your object is already set by a by group this will add it as the next by group.

        Args
        ----
            bracket:`str, (optional)`: Filter by income. Defaults to None.

        Returns
        -------
            Spark DataFrame: :class:`Paletable`: returns the updated object

        Examples
        --------
        >>> Enrollment.byIncomeBracket('01')
        or
        >>> Trend.byIncomeBracket('01-03')
        or
        >>> Trend.byIncomeBracket('02,03,05')
        """

        self.palet.logger.info('Group by - income bracket')

        self._addByGroup(PaletMetadata.Enrollment.identity.income)
        if bracket is not None:
            self.filter.update({PaletMetadata.Enrollment.identity.income: "'" + bracket + "'"})
        else:
            self.filter.update({PaletMetadata.Enrollment.identity.income: "null"})

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byYear(self, year: int = None, count: int = 1):
        """Filter your query by Year. Most top level objects inherit this function such as Enrollment, Trend, etc.

        Args:
            year:`int, (optional)`: Filter by year using the year in numerical format. Defaults to None.
            count:`int, (optional)`: Specify the number of years before or after the year specified. Defaults to 1.

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object
        """
        self.timeunit = 'year'
        self.timeunitvalue = year
        self.lookback = count

        if year is not None:
            self.filter.update({PaletMetadata.Enrollment.fileDate: "'" + year + "'"})

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byMonth(self, month: int = None):
        """Filter your query by Month. Most top level objects inherit this function such as Enrollment, Trend, etc.

        Args:
            month:`int, (optional)`: Filter by month using the month in numerical format. Defaults to None.

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object
        """
        self.timeunit = 'month'
        self.timeunitvalue = month

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def sql(self):
        """The SQL query that the Enrollment class uses to pull dataframes.

        This can be called allowing an analyst to view the SQL the Enrollment is using.

        Args:
            None: No input required.

        Returns:
            str: Returns a text string containing the SQL query run by the Enrollment class.

        Example:
            Create object containing the SQL query:

            >>> q = Enrollment().byState().sql()

            Return the query as text:

            >>> print(q)

            Alternatively

            >>> print(Enrollment().byState().sql())

        """

        return self.sql

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
        from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, LongType, DoubleType

        # self.palet.logger.info('Fetching data - \n' + self.sql())

        sparkDF = session.sql(self.sql())

        if (sparkDF is not None):

            if PaletMetadata.Enrollment.raceEthnicity.ethnicity in sparkDF.columns:
                sparkDF = sparkDF.withColumn(
                    PaletMetadata.Enrollment.raceEthnicity.ethnicity,
                    sparkDF[PaletMetadata.Enrollment.raceEthnicity.ethnicity].cast(StringType()))

        df = sparkDF.toPandas()

        if PaletMetadata.Enrollment.raceEthnicity.ethnicity in df.columns:
            df[PaletMetadata.Enrollment.raceEthnicity.ethnicity] \
                = df[PaletMetadata.Enrollment.raceEthnicity.ethnicity].astype(pd.StringDtype())
            df[PaletMetadata.Enrollment.raceEthnicity.ethnicity].fillna('6', inplace=True)

        # perform data enrichments
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
