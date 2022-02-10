"""
The Paletable module contains the Paletable class and its attributes. Said attributes can be combined with top
level objects, such as enrollment, to filter data by a variety of meterics. These metrics include age range,
ethnicity, file data, income bracket, gender and state. Paletable also contains fetch(), the attribute used to
return datafranes created by high level objects.
"""

import pandas as pd

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

        self.palet.logger.info('Initializing Coverage API')

    # -----------------------------------------------------------------------
    #
    #  Multiple steps to create the coverage_type views
    #  call this function with step 1, 2, or 3
    #  They should be called in order for safety
    #
    # -----------------------------------------------------------------------
    def _transposeCoverageType(self):

        from pyspark.sql import SparkSession
        session = SparkSession.getActiveSession()

        # session.sql("""
        #     create view if not exists compress_coverage_type as
        #     select
        #         a.de_fil_dt,
        #         a.mc_plan_type_cd_01,
        #         a.mc_plan_type_cd_02,
        #         a.mc_plan_type_cd_03,
        #         a.mc_plan_type_cd_04,
        #         a.mc_plan_type_cd_05,
        #         a.mc_plan_type_cd_06,
        #         a.mc_plan_type_cd_07,
        #         a.mc_plan_type_cd_08,
        #         a.mc_plan_type_cd_09,
        #         a.mc_plan_type_cd_10,
        #         a.mc_plan_type_cd_11,
        #         a.mc_plan_type_cd_12,
        #         sum(case when a.mdcd_enrlmt_days_01 > 0 then 1 else 0 end) as mdcd_enrlmt_days_01,
        #         sum(case when a.mdcd_enrlmt_days_02 > 0 then 1 else 0 end) as mdcd_enrlmt_days_02,
        #         sum(case when a.mdcd_enrlmt_days_03 > 0 then 1 else 0 end) as mdcd_enrlmt_days_03,
        #         sum(case when a.mdcd_enrlmt_days_04 > 0 then 1 else 0 end) as mdcd_enrlmt_days_04,
        #         sum(case when a.mdcd_enrlmt_days_05 > 0 then 1 else 0 end) as mdcd_enrlmt_days_05,
        #         sum(case when a.mdcd_enrlmt_days_06 > 0 then 1 else 0 end) as mdcd_enrlmt_days_06,
        #         sum(case when a.mdcd_enrlmt_days_07 > 0 then 1 else 0 end) as mdcd_enrlmt_days_07,
        #         sum(case when a.mdcd_enrlmt_days_08 > 0 then 1 else 0 end) as mdcd_enrlmt_days_08,
        #         sum(case when a.mdcd_enrlmt_days_09 > 0 then 1 else 0 end) as mdcd_enrlmt_days_09,
        #         sum(case when a.mdcd_enrlmt_days_10 > 0 then 1 else 0 end) as mdcd_enrlmt_days_10,
        #         sum(case when a.mdcd_enrlmt_days_11 > 0 then 1 else 0 end) as mdcd_enrlmt_days_11,
        #         sum(case when a.mdcd_enrlmt_days_12 > 0 then 1 else 0 end) as mdcd_enrlmt_days_12,
        #         sum(case when a.chip_enrlmt_days_01 > 0 then 1 else 0 end) as chip_enrlmt_days_01,
        #         sum(case when a.chip_enrlmt_days_02 > 0 then 1 else 0 end) as chip_enrlmt_days_02,
        #         sum(case when a.chip_enrlmt_days_03 > 0 then 1 else 0 end) as chip_enrlmt_days_03,
        #         sum(case when a.chip_enrlmt_days_04 > 0 then 1 else 0 end) as chip_enrlmt_days_04,
        #         sum(case when a.chip_enrlmt_days_05 > 0 then 1 else 0 end) as chip_enrlmt_days_05,
        #         sum(case when a.chip_enrlmt_days_06 > 0 then 1 else 0 end) as chip_enrlmt_days_06,
        #         sum(case when a.chip_enrlmt_days_07 > 0 then 1 else 0 end) as chip_enrlmt_days_07,
        #         sum(case when a.chip_enrlmt_days_08 > 0 then 1 else 0 end) as chip_enrlmt_days_08,
        #         sum(case when a.chip_enrlmt_days_09 > 0 then 1 else 0 end) as chip_enrlmt_days_09,
        #         sum(case when a.chip_enrlmt_days_10 > 0 then 1 else 0 end) as chip_enrlmt_days_10,
        #         sum(case when a.chip_enrlmt_days_11 > 0 then 1 else 0 end) as chip_enrlmt_days_11,
        #         sum(case when a.chip_enrlmt_days_12 > 0 then 1 else 0 end) as chip_enrlmt_days_12
        #     from
        #         taf.taf_ann_de_base as a
        #     where
        #         a.da_run_id in ( 6279, 6280 )
        #         and a.de_fil_dt = 2021
        #     group by
        #         de_fil_dt,
        #         a.mc_plan_type_cd_01,
        #         a.mc_plan_type_cd_02,
        #         a.mc_plan_type_cd_03,
        #         a.mc_plan_type_cd_04,
        #         a.mc_plan_type_cd_05,
        #         a.mc_plan_type_cd_06,
        #         a.mc_plan_type_cd_07,
        #         a.mc_plan_type_cd_08,
        #         a.mc_plan_type_cd_09,
        #         a.mc_plan_type_cd_10,
        #         a.mc_plan_type_cd_11,
        #         a.mc_plan_type_cd_12
        #     order by
        #         de_fil_dt,
        #         a.mc_plan_type_cd_01,
        #         a.mc_plan_type_cd_02,
        #         a.mc_plan_type_cd_03,
        #         a.mc_plan_type_cd_04,
        #         a.mc_plan_type_cd_05,
        #         a.mc_plan_type_cd_06,
        #         a.mc_plan_type_cd_07,
        #         a.mc_plan_type_cd_08,
        #         a.mc_plan_type_cd_09,
        #         a.mc_plan_type_cd_10,
        #         a.mc_plan_type_cd_11,
        #         a.mc_plan_type_cd_12
        # """)

        session.sql("""
            create view if not exists pivoted_coverage as
            select
            a.de_fil_dt,
            stack(12,
                1, a.mc_plan_type_cd_01, a.mdcd_enrlmt_days_01, a.chip_enrlmt_days_01,
                2, a.mc_plan_type_cd_02, a.mdcd_enrlmt_days_02, a.chip_enrlmt_days_02,
                3, a.mc_plan_type_cd_03, a.mdcd_enrlmt_days_03, a.chip_enrlmt_days_03,
                4, a.mc_plan_type_cd_04, a.mdcd_enrlmt_days_04, a.chip_enrlmt_days_04,
                5, a.mc_plan_type_cd_05, a.mdcd_enrlmt_days_05, a.chip_enrlmt_days_05,
                6, a.mc_plan_type_cd_06, a.mdcd_enrlmt_days_06, a.chip_enrlmt_days_06,
                7, a.mc_plan_type_cd_07, a.mdcd_enrlmt_days_07, a.chip_enrlmt_days_07,
                8, a.mc_plan_type_cd_08, a.mdcd_enrlmt_days_08, a.chip_enrlmt_days_08,
                9, a.mc_plan_type_cd_09, a.mdcd_enrlmt_days_09, a.chip_enrlmt_days_09,
                10, a.mc_plan_type_cd_10, a.mdcd_enrlmt_days_10, a.chip_enrlmt_days_10,
                11, a.mc_plan_type_cd_11, a.mdcd_enrlmt_days_11, a.chip_enrlmt_days_11,
                12, a.mc_plan_type_cd_12, a.mdcd_enrlmt_days_12, a.chip_enrlmt_days_12
                ) as (month, coverage_type, mdcd_enrlmt, chip_enrlmt)
            from
                palet_mart.compress_coverage_type as a
        """)

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
            age_range: `str, optional`: Filter a single age, range such as
            18-21, or an inclusive number such as 65+. Defaults to None.

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

        # self._addPreProcess(self._transposeCoverageType)

        z = """
                select
                    a.de_fil_dt,
                    a.month,
                    a.coverage_type,
                    sum(a.mdcd_enrlmt) as mdcd_enrlmt,
                    sum(a.chip_enrlmt) as chip_enrlmt
                from
                    palet_mart.pivoted_coverage as a
                where
                    coverage_type is not null
                group by
                    a.de_fil_dt,
                    a.coverage_type,
                    a.month
                order by
                    a.de_fil_dt,
                    a.coverage_type,
                    a.month
            """
        #  TODO: {self._getRunIds()}
        #  TODO: {self._defineWhereClause()}
        #  TODO: {self._getByGroupWithAlias()}

        # compress rows from coverage if it is in the by group
        self._addPostProcess(self._buildValueColumn)
        # self._addPostProcess(self._percentChange)
        # self._addPostProcess(self._decorate)

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

        # post-processing callbacks
        for pp in self.preprocesses:
            pp()

        # raw results
        sparkDF = session.sql(self.sql())
        df = sparkDF.toPandas()

        # post-processing callbacks
        for pp in self.postprocesses:
            df = pp(df)

        # df = df.drop(columns=['isfirst'])

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
