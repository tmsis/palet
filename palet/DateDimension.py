from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DecimalType, IntegerType, LongType, DoubleType
from datetime import date, datetime, timedelta

import pandas as pd
from dateutil.relativedelta import relativedelta
"""
The DateDimension module's primary purpose is to generate the relevant run id's for Paletable objects. It consists of the DateDimension class and
its attributes.
"""


# -----------------------------------------------------------------------
#
# -----------------------------------------------------------------------
class DateDimension(object):
    """
    The date dimension class consists of an SQL query that generates every possible run id pertaining to enrollment, chronic conditions, claims and more.
    This class also contains :meth:`DateDimension.DateDimension.relvant_runids` which automatically returns run ids for a given file type and lookback period.
    Additionally, this class contains :meth:`DateDimension.DateDimension.usingRunIds` which an analyst can utilize to manually specify the run ids to include
    in the query. Run ids can also be manually set by year using using the DateDimension object. Examples of both methods are present below.

    Examples:
        Import DateDimension

        >>> from palet.DateDimension import DateDimension

        Specify the year you would like to focus on

        >>> DateDimension(years=[2019])

        Or specify multiple years

        >>> DateDimension(years=[2019,2020])

        Now run a Paletable object

        >>> api = Enrollment()

        Convert the query to a DataFrame and return

        >>> df = api.fetch()

        >>> display(df)

    """

    # -----------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------
    __instance = None

    # -----------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------
    def __new__(cls, asOf: date = None, runIds: list = None, years: list = None, months: list = None):
        if not DateDimension.__instance:
            # cls.__instance = super().__new__(cls)
            DateDimension.__instance = object.__new__(cls)

            if asOf is None:
                asOf = datetime.now().replace(day=1).date()

            DateDimension.__instance.asOf = asOf
            DateDimension.__instance.runIds = runIds
            DateDimension.__instance.years = years
            DateDimension.__instance.months = months

        return DateDimension.__instance

    # -----------------------------------------------------------------------
    #
    # DateDimension Singleton
    #
    # -----------------------------------------------------------------------
    @staticmethod
    def getInstance():
        if DateDimension.__instance is None:
            return DateDimension()
        else:
            return DateDimension.__instance

    # -----------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------
    def __init__(self, asOf: date = None, runIds: list = None, years: list = None, months: list = None, period: str = 'month'):
        from pyspark.sql.functions import col, substring, length, when, upper, to_date, concat

        self._timeunit = period

        z = """
            select distinct
                fil_4th_node_txt,
                otpt_name,
                max(da_run_id) as da_run_id,
                rptg_prd,
                fil_dt
            from
                taf.efts_fil_meta
            where
                ltst_run_ind = true
                and otpt_name in ('TAF_ANN_DE_BASE',
                                  'TAF_IPH',
                                  'TAF_LTH',
                                  'TAF_OTH',
                                  'TAF_RXH')
            group by
                fil_4th_node_txt,
                otpt_name,
                fil_dt,
                rptg_prd
            order by
                fil_4th_node_txt,
                otpt_name,
                fil_dt desc,
                rptg_prd
        """
        spark = SparkSession.getActiveSession()
        if spark is not None:
            spark_df = spark.sql(z)
            spark_df = spark_df.withColumn('fil_4th_node_txt', spark_df['fil_4th_node_txt'].cast(StringType()))
            spark_df = spark_df.withColumn('otpt_name', spark_df['otpt_name'].cast(StringType()))
            spark_df = spark_df.withColumn('da_run_id', spark_df['da_run_id'].cast(LongType()))
            spark_df = spark_df.withColumn('rptg_prd', spark_df['rptg_prd'].cast(StringType()))
            spark_df = spark_df.withColumn('fil_dt', spark_df['fil_dt'].cast(StringType()))

            spark_df = spark_df.withColumn('yyyy', substring('fil_dt', 1, 4))
            spark_df = spark_df.withColumn('mmlen', length('fil_dt'))
            spark_df = spark_df.withColumn('mm', when(col('mmlen') == 6, substring('fil_dt', 5, 2)).otherwise('01'))
            spark_df = spark_df.withColumn('mmm', upper(when(col('mmlen') == 6, substring('rptg_prd', 1, 3)).otherwise('JAN')))
            spark_df = spark_df.withColumn('year', col('yyyy').cast(IntegerType()))
            spark_df = spark_df.withColumn('month', col('mm').cast(IntegerType()))
            spark_df = spark_df.na.fill(value=1, subset=['month'])
            spark_df = spark_df.withColumn('dt_yearmon', to_date(concat(col('yyyy'),col('mm')), 'yyyymm'))

            self.spark_df = spark_df
            if years is not None:
                self.spark_df = self.spark_df.filter(self.spark_df.year.isin(years))
            if months is not None:
                self.spark_df = self.spark_df.filter(self.spark_df.month.isin(months))

            self.df = spark_df.toPandas()

        else:
            self.df = None

        if asOf is not None:
            DateDimension.__instance.asOf = asOf
        if runIds is not None:
            DateDimension.__instance.runIds = runIds

    # -----------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------
    def relevant_runids(self, taf_file_type, lookback: int = None):
        """
        This function is utilized in the SQL query generated by Paletable objects to automatically ensure the analyst's query is focusing
        on the correct run ids for the given context. This function can also be used alone to return a list of of relevant run ids in
        string format.

        Args:
            taf_file_type: `str`: Three letter acronym for the file type relevant to the query. 'BSE', 'IPH', etc.
            lookback: `int`: Enter an integer for the number of years you want to be included from from the most recent year.
            6 will include 2022 and the 5 years before it.

        Returns:
            str: Returns a list of the relevant run ids in string format.

        Note:
            Users will likely not interact with this function directly, as it is automatically run when Paletable objects are

        Example:
            Import DateDimension

            >>> from palet.DateDimension import DateDimension

            Create a DateDimension object with this function and the relevant arguements

            >>> DateDimension().relevant_runids('BSE', 6)

            Now run a Paletable object with the run ids entered as an arguement of the object

            >>> api = Enrollment([7072])

            Convert the query to a DataFrame and return

            >>> df = api.fetch()

            >>> display(df)

        """

        if lookback is None:
            if taf_file_type == 'BSE':
                lookback = 5
            else:
                lookback = 60

        dt = self.asOf

        if taf_file_type == 'BSE':
            if self.runIds is not None:
                return ','.join(map(str, self.runIds))
            dt = dt - relativedelta(years=lookback)
        else:
            dt = dt - relativedelta(months=lookback)

        if self.df is not None:

            df = self.df

            # ---------------------------------------------------------
            # PySpark Version
            # ---------------------------------------------------------
            # if self.years is not None:
            #     df = df.filter(df.year.isin(self.years))
            # if self.months is not None:
            #     df = df.filter(df.month.isin(self.months))

            # df = df[(df['dt_yearmon'] >= dt) &
            #         (df['dt_yearmon'] <= self.asOf) &
            #         (df['fil_4th_node_txt'] == taf_file_type)]

            # rids = df.select('da_run_id').rdd.flatMap(lambda x: x).collect()

            # ---------------------------------------------------------
            # Pandas Version
            # ---------------------------------------------------------
            if self.years is not None:
                df = df[df['year'].isin(self.years)]
            if self.months is not None:
                df = df[df['month'].isin(self.months)]

            rids = df[(df['dt_yearmon'] >= dt) &
                      (df['dt_yearmon'] <= self.asOf) &
                      (df['fil_4th_node_txt'] == taf_file_type)]['da_run_id']

        else:
            rids = ()

        return ','.join(map(str, rids))

    # ---------------------------------------------------------------------------------
    #
    # Use this method to pass in user defined runIds
    #
    # ---------------------------------------------------------------------------------
    def usingRunIds(self, ids: list = None):
        """For users who which to pass in their own Run Ids, call this method by passing
        in a list of run ids separated by comma. e.g. [6279, 6280]

        Args:
            ids: `list, optional`: Filter by specific runids by passing in a list of one or more.
            default: `none`: Defaults to an Empty List [] and will clear user defined run ids when called by default

        Returns:
            No return values

        Example:
            Start with a Paletable object:

            >>> api = Enrollment()

            Specify run ids:

            >>> api.usingRunIds([6279, 6280])

            Return DataFrame:

            >>> display(api.fetch())

            Alternatively enter the list of run ids as a parameter of the Paletable object:

            >>> api = Enrollment([6279, 6280])

            Return DataFrame:

            >>> display(api.fetch())

        """
        # self.palet.logger.debug("using RunIds: " + str(ids))
        if ids is not None:
            self._user_runids = ids
        else:
            self._user_runids = None

        return self
