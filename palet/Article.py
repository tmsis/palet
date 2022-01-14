import pandas as pd
from datetime import datetime
import logging

from pyspark.sql import SparkSession
from palet.Palet import Palet


class Article:

    # TODO: Continue to clean up docstring using syntax formatting
    # Initialize the comann variables here.
    # All SQL objects should inherit from this class
    # ----------------------------------------------
    def __init__(self):
        self.by = {}
        self.by_group = []
        self.filter = {}
        self.where = []

        self.month_group = []
        self._str_month_ = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
        self._chip_enrlmt_by_month_ = {
            '01': 'chip_enrlmt_days_01',
            'jan': 'chip_enrlmt_days_01',
            '02': 'chip_enrlmt_days_02',
            'feb': 'chip_enrlmt_days_02',
            '03': 'chip_enrlmt_days_03',
            'mar': 'chip_enrlmt_days_03',
            '04': 'chip_enrlmt_days_04',
            'apr': 'chip_enrlmt_days_04',
            '05': 'chip_enrlmt_days_05',
            'may': 'chip_enrlmt_days_05',
            '06': 'chip_enrlmt_days_06',
            'jun': 'chip_enrlmt_days_06',
            '06': 'chip_enrlmt_days_06',
            'jul': 'chip_enrlmt_days_07',
            '07': 'chip_enrlmt_days_07',
            'aug': 'chip_enrlmt_days_08',
            '08': 'chip_enrlmt_days_08',
            'sep': 'chip_enrlmt_days_09',
            '09': 'chip_enrlmt_days_09',
            'oct': 'chip_enrlmt_days_10',
            '10': 'chip_enrlmt_days_10',
            'nov': 'chip_enrlmt_days_11',
            '11': 'chip_enrlmt_days_11',
            'dec': 'chip_enrlmt_days_12',
            '12': 'chip_enrlmt_days_12'
        }
        self._mdcd_enrlmt_by_month_ = {
            '01': 'mdcd_enrlmt_days_01',
            'jan': 'mdcd_enrlmt_days_01',
            '02': 'mdcd_enrlmt_days_02',
            'feb': 'mdcd_enrlmt_days_02',
            '03': 'mdcd_enrlmt_days_03',
            'mar': 'mdcd_enrlmt_days_03',
            '04': 'mdcd_enrlmt_days_04',
            'apr': 'mdcd_enrlmt_days_04',
            '05': 'mdcd_enrlmt_days_05',
            'may': 'mdcd_enrlmt_days_05',
            '06': 'mdcd_enrlmt_days_06',
            'jun': 'mdcd_enrlmt_days_06',
            '07': 'mdcd_enrlmt_days_07',
            'jul': 'mdcd_enrlmt_days_07',
            '08': 'mdcd_enrlmt_days_08',
            'aug': 'mdcd_enrlmt_days_08',
            '09': 'mdcd_enrlmt_days_09',
            'sep': 'mdcd_enrlmt_days_09',
            '10': 'mdcd_enrlmt_days_10',
            'oct': 'mdcd_enrlmt_days_10',
            '11': 'mdcd_enrlmt_days_11',
            'nov': 'mdcd_enrlmt_days_11',
            '12': 'mdcd_enrlmt_days_12',
            'dec': 'mdcd_enrlmt_days_12'
        }

        self.postprocess = []

        # TODO: remove this logic when ready
        self.palet = Palet('201801')
        self._pctChangePeriod = -1
        self._monthly_cnt_stmt = None
        # This variable exists to save the sql statement from the sub-classbyes
        # self._sql = None

    # ---------------------------------------------------------------------------------
    #   getByGroupWithAlias: This function allows our byGroup to be aliased
    #       properly for the dynamic sql generation
    # ---------------------------------------------------------------------------------
    def _getByGroupWithAlias(self):
        z = ""
        new_line_comma = '\n\t\t\t   ,'
        if (len(self.by_group)) > 0:
            for column in self.by_group:
                z += "ann." + column + new_line_comma
            return f"{z}"
        else:
            return ''

    # call this if they want monthly counts
    # TODO: I'm not sure the conditional is needed here
    # TODO: Review the OVER() call for counts in here. We need a view.
    # At first I thought it was needed in the GroupBy but calculated columns don't need a groupBy
    def _enroll_by_state_logic(self, logicType="count"):
        self._monthly_cnt_stmt = ""
        new_line_comma = ',\n\t\t\t\t\t'
        if logicType == "count":
            for monthFld in self._str_month_:
                self._monthly_cnt_stmt += "sum(case when " + "ann." + self._chip_enrlmt_by_month_[monthFld] + " > 0 or ann." + self._mdcd_enrlmt_by_month_[monthFld] + " > 0 then 1 else 0 end) OVER() as " + monthFld + "_enrlmt_cnt" + new_line_comma
            return self._monthly_cnt_stmt
        elif logicType == "prefix":
            for monthFld in self.month_group:
                self._monthly_cnt_stmt += "ann." + monthFld + new_line_comma
            return self._monthly_cnt_stmt

    # Create a temporary table here to optimize our querying of the
    # objects and data
    def _createView_rid_x_annth_x_state(self):

        # create or replace temporary view rid_x_annth_x_state as
        # TODO: remove the hard coded file data below (2018)
        z = """
            (select
            max(da_run_id) as max_da_run_id
            from
            taf.taf_ann_de_base
            where
            DE_FIL_DT = 2018
            AND (
            mdcd_enrlmt_days_yr > 1
            OR chip_enrlmt_days_yr > 1
            ))"""
        return z

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
                        clause = ("ann." + key, value)
                        where.append(' ((= '.join(clause))

                # Check for multiples with , separator
                elif str(values).find(",") > -1:
                    splitVals = self._checkForMultiVarFilter(values, ",")
                    for values in splitVals:
                        # check for age ranges here with the - separator
                        if str(values).find("-") > -1:
                            splitRange = self._checkForMultiVarFilter(values, "-")
                            range_stmt = "ann." + key + " between " + splitRange[0] + " and " + splitRange[1]
                        # check for greater than; i.e. x+ equals >= x
                        elif str(values).find("+") > -1:
                            range_stmt = "ann." + key + " >= " + values.strip("+")
                        # take the x+ and strip out the +
                        where.append(range_stmt)

                else:  # else parse the single value
                    clause = ("ann." + key, self.filter[key])
                    where.append(' = '.join(clause))

            return f"where {' and '.join(where)}"

        else:
            return "where 1=1"

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _checkForMultiVarFilter(self, values: str, separator=" "):
        return values.split(separator)

    def _percentChange(self, df):
        print('_percentChange')

        # this appears to have to be a key variable based on user input. We need to look at it more
        df['enrollment change'] = df['enrollment'].pct_change(self._pctChangePeriod)

        return df

    def _decorate(self, df):
        print('_decorate')

        df['USPS'] = df['SUBMTG_STATE_CD'].apply(lambda x: str(x).zfill(2))
        df = pd.merge(df, self.palet.st_name,
                      how='inner',
                      left_on=['USPS'],
                      right_on=['USPS'])

        return df

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
        self.by_group.append("DE_FIL_DT")

        if fileDate is not None:
            self.filter.update({"DE_FIL_DT": "'" + fileDate + "'"})

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

    def sql(self):

        rms = self._createView_rid_x_annth_x_state()
        ebs = self._enroll_by_state_logic()
        pref = self._enroll_by_state_logic("prefix")

        # new_line_comma = '\n\t\t\t   ,'

        # Do we need to defaul to byState regardless? Is everything State reliant?
        # If we don't have submtg_state_cd any call fails so we're forcing it in
        # If the user decides to use it as their own byGroup we need to make sure
        # not to add it twice
        if 'SUBMTG_STATE_CD' not in self.by_group:
            self = self.byState()

        z = f"""
                select
                    {self._getByGroupWithAlias()}
                    ann.DE_FIL_DT,
                    {ebs}
                    count(*) as num_enrolled
                from
                    taf.taf_ann_de_base as ann
                {self._defineWhereClause()}
                AND ann.da_run_id in
                {rms}
                AND
                sum(
                    case
                    when ann.chip_enrlmt_days_yr > 0
                    or ann.mdcd_enrlmt_days_yr > 0 then 1
                    else 0
                    end
                ) as total_enrlmt_eoy
                group by
                   {self._getByGroupWithAlias()}
                    ann.DE_FIL_DT
                order by
                    {self._getByGroupWithAlias()}
                    ann.DE_FIL_DT
            """
        self.postprocess.append(self._percentChange)
        self.postprocess.append(self._decorate)
        # self._sql = z
        return z

    # ---------------------------------------------------------------------------------
    def fetch(self):
        """Call this function when you are ready for results

        Returns:
            :class:`Dataframe`: Executes your query and returns a spark dataframe object.
        """
        session = SparkSession.getActiveSession()
        # self.palet.logger.info('Fetching data - \n' + self.sql())

        sparkDF = session.sql(self.sql())
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
# purpose of contributing to a comanns of creative, cultural and scientific
# works ("Comanns") that the public can reliably and without fear of later
# claims of infringement build upon, modify, incorporate in other works, reuse
# and redistribute as freely as possible in any form whatsoever and for any
# purposes, including without limitation commercial purposes. These owners may
# contribute to the Comanns to promote the ideal of a free culture and the
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

#   d. Affirmer understands and acknowledges that Creative Comanns is not a
#   party to this document and has no duty or obligation with respect to this
#   CC0 or use of the Work.

# For more information, please see
# <http://creativecomanns.org/publicdomain/zero/1.0/>
