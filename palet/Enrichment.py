
import pandas as pd
from palet.PaletMetadata import PaletMetadata
import palet.Palet as Palet


class Enrichment():

    def __init__(self) -> None:
        self.palet = Palet('201801')

    def getDefinedColumns(self):
        self.defined_columns = {
            'isfirst': Enrichment._removeIsFirst,
            'age_grp_flag': Enrichment._buildAgeGroupColumn,
            'race_ethncty_flag': Enrichment._buildRaceEthnicityColumn,
            'SUBMTG_STATE_CD': Enrichment._mergeStateEnrollments,
            'race_ethncty_exp_flag': Enrichment._buildRaceEthnicityExpColumn,
            'ethncty_cd': Enrichment._buildEthnicityColumn,
            'enrl_type_flag': Enrichment._buildEnrollmentType,
            'elgblty_grp_cd': Enrichment._buildEligibilityType,
            'incm_cd': Enrichment._buildIncomeColumn
        }

        return self.defined_columns

    # --------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _mergeStateEnrollments(df: pd.DataFrame):

        # logging.debug('Merging separate state enrollments')
        timeunit = 'month'

        if 'year' in df.columns:
            timeunit = 'year'

        df['USPS'] = df['SUBMTG_STATE_CD'].apply(lambda x: str(x).zfill(2))
        df = pd.merge(df, Palet.st_name,
                      how='inner',
                      left_on=['USPS'],
                      right_on=['USPS'])
        df = pd.merge(df, Palet.st_usps,
                      how='inner',
                      left_on=['USPS'],
                      right_on=['USPS'])
        df = df.drop(['USPS'], axis=1)

        df.groupby(by=['STABBREV', 'de_fil_dt', timeunit]).sum().reset_index()

        return df

    # --------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _findRaceValueName(x):
        # # self.palet.logger.debug('looking up the race_ethncty_flag value from our metadata')
        # import math
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
    def _buildRaceEthnicityColumn(df: pd.DataFrame):
        # self.palet.logger.debug('build our columns by looking for race value')
        df['race'] = df.apply(lambda x: Enrichment._findRaceValueName(x), axis=1)

        return df

    # --------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _findRaceExpValueName(x):
        # self.palet.logger.debug('looking up the race_ethncty_exp_flag value from our metadata')
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
    def _buildRaceEthnicityExpColumn(df: pd.DataFrame):
        # self.palet.logger.debug('build our columns by looking for race_ethncty_exp_flag')
        df['raceExpanded'] = df.apply(lambda x: Enrichment._findRaceExpValueName(x), axis=1)

        return df

    # --------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _findEthnicityValueName(x):
        # self.palet.logger.debug('looking up the ethncty_cd value from our metadata')
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
    def _buildEthnicityColumn(df: pd.DataFrame):
        # self.palet.logger.debug('build our columns by looking for ethncty_cd')
        print("calling build race ethnicity")
        df['ethnicity'] = df.apply(lambda x: Enrichment._findEthnicityValueName(x), axis=1)

        return df

    # --------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _findEnrollmentType(x):
        # self.palet.logger.debug('looking up the enrollmentType value from our metadata')
        # get this row's ref value from the column by name
        y = x[PaletMetadata.Enrollment.derived_enrollment_field]
        # lookup label with value
        return PaletMetadata.Enrollment.chip_cd.get(y)

    def _buildEnrollmentType(df: pd.DataFrame):
        # self.palet.logger.debug('build our columns by looking for enrollmentType')
        df['enrollment_type'] = df.apply(lambda x: Enrichment._findEnrollmentType(x), axis=1)

        return df

    # --------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _findEligibiltyType(x):
        # self.palet.logger.debug('looking up the eligibility value from our metadata')
        # get this row's ref value from the column by name
        y = x[PaletMetadata.Eligibility.eligibiltyGroup]
        # lookup label with value
        return PaletMetadata.Eligibility.eligibility_cd.get(y)

    # --------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _buildEligibilityType(df: pd.DataFrame):
        print(df)
        # self.palet.logger.debug('build our columns by looking for eligibilty codes')
        df['eligibility_category'] = df.apply(lambda x: Enrichment._findEligibiltyType(x), axis=1)
        return df

    # --------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _findIncomeValueName(x):
        # self.palet.logger.debug('looking up the incm_cd value from our metadata')
        # get this row's ref value from the column by name
        y = x[PaletMetadata.Enrollment.identity.income]
        # lookup label with value
        return PaletMetadata.Enrollment.identity.incm_cd.get(y)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _buildIncomeColumn(df: pd.DataFrame):
        # self.palet.logger.debug('build our columns by looking for income_cd')
        df['income'] = df.apply(lambda x: Enrichment._findIncomeValueName(x), axis=1)

        return df

    # --------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _findAgeGroupValueName(x):
        # get this row's ref value from the column by name
        y = x[PaletMetadata.Enrollment.identity.ageGroup]
        # lookup label with value
        return PaletMetadata.Enrollment.identity.age_grp_flag.get(y)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _buildAgeGroupColumn(df: pd.DataFrame):
        df['ageGroup'] = df.apply(lambda x: Enrichment._findAgeGroupValueName(x), axis=1)

        return df

    # --------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _findValueName_(x, column, derived_column):
        # get this row's ref value from the column by name
        y = x[column]
        # if the value is NaN, default to unknown
        if y is None or y == 'null':
            return 'unknown'
        else:
            # lookup label with value
            return derived_column.get(y)

    # ---------------------------------------------------------------------------------
    # _buildValueColumn_ is provided to convert codes to values
    # for **kwargs use columnToAdd1, columnToAdd2, etc.
    # ---------------------------------------------------------------------------------
    def _buildValueColumn_(df: pd.DataFrame, **kwargs):
        column1 = kwargs[0]
        df[column1] = df.apply(lambda x: Enrichment._findValueName_(x), axis=1)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ----------------------------------------------------------------------------------
    def _removeIsFirst(df: pd.DataFrame):
        df = df.drop(columns=['isfirst'])
        return df
