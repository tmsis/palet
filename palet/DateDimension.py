from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DecimalType, IntegerType, LongType, DoubleType
from datetime import date, datetime, timedelta
import pandas as pd
from dateutil.relativedelta import relativedelta


# -----------------------------------------------------------------------
#
# -----------------------------------------------------------------------
class DateDimension:

    # -----------------------------------------------------------------------
    #
    # -----------------------------------------------------------------------
    def __init__(self, as_of_date: date = None, runIds: list = None):
        z = """
            select distinct
                fil_4th_node_txt,
                otpt_name,
                da_run_id,
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
                da_run_id,
                rptg_prd
            order by
                fil_4th_node_txt,
                otpt_name,
                fil_dt desc,
                da_run_id,
                rptg_prd
            ;
        """
        spark = SparkSession.getActiveSession()
        if spark is not None:
            spark_df = spark.sql(z)
            spark_df = spark_df.withColumn('fil_4th_node_txt', spark_df['fil_4th_node_txt'].cast(StringType()))
            spark_df = spark_df.withColumn('otpt_name', spark_df['otpt_name'].cast(StringType()))
            spark_df = spark_df.withColumn('da_run_id', spark_df['da_run_id'].cast(LongType()))
            spark_df = spark_df.withColumn('rptg_prd', spark_df['rptg_prd'].cast(StringType()))
            spark_df = spark_df.withColumn('fil_dt', spark_df['fil_dt'].cast(StringType()))

            df = spark_df.toPandas()

            df['yyyy'] = df['fil_dt'].str[0:4]
            df['mmlen'] = df['fil_dt'].apply(len)
            df['mm'] = df.apply(lambda x: x['fil_dt'][4:6] if x['mmlen'] == 6 else '01', axis=1)
            df['mmm'] = df.apply(lambda x: x['rptg_prd'].str[0:3].upper() if x['mmlen'] == '6' else 'JAN', axis=1)
            df['year'] = pd.to_numeric(df['yyyy'])
            df['month'] = pd.to_numeric(df['mm'])
            df['month'].fillna(1, inplace=True)
            df['dt_yearmon'] = df.apply(lambda x: date(x['year'], int(x['month']), 1), axis=1)
            self.df = df

        else:
            self.df = None

        if as_of_date is None:
            as_of_date = datetime.now().replace(day=1).date()

        self.as_of_date = as_of_date

        self.runIds = runIds

    # -----------------------------------------------------------------------
    #
    # -----------------------------------------------------------------------
    def relevant_runids(self, taf_file_type, lookback):

        dt = self.as_of_date

        if taf_file_type == 'BSE':
            if self.runIds is not None:
                return ','.join(map(str, self.runIds))
            dt = dt - relativedelta(years=lookback)
        else:
            dt = dt - relativedelta(months=lookback)

        if self.df is not None:
            rids = self.df[(self.df['dt_yearmon'] >= dt) & (self.df['dt_yearmon'] <= self.as_of_date) &
                                                           (self.df['fil_4th_node_txt'] == taf_file_type)]['da_run_id']

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
        self.palet.logger.debug("using RunIds: " + str(ids))
        if ids is not None:
            self._user_runids = ids
        else:
            self._user_runids = None

        return self
