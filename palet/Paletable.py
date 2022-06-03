"""
The Paletable module contains the Paletable class and its attributes. Said attributes can be combined with top
level objects, such as enrollment, to filter data by a variety of meterics. These metrics include age range,
ethnicity, file data, income bracket, gender and state. Paletable also contains fetch(), the attribute used to
return datafranes created by high level objects.
"""

from typing import overload
import pandas as pd
from palet.DateDimension import DateDimension
from palet.Palet import Palet
from palet.PaletMetadata import PaletMetadata
from datetime import date


class Paletable():
    """
    Class containing attributes that can be combined with high level objects (or Paletable objects) from the PALET library. These
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

    # Initialize the comann variables here.
    # All SQL objects should inherit from this class
    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, asOf: date = None, runIds: list = None):

        self.timeunit = None
        self.by_group = []
        self.filter = {}
        self.filter_by_type = {}
        self.age_band = None
        self.derived_by_type_group = []
        self.aggregate_group = []

        self.date_dimension = DateDimension.getInstance()

        self.preprocesses = []
        self.postprocesses = []

        self.outer_joins = []

        self.calculations = []

        self.markers = {}
        self.having_constraints = []
        self._outersql = {}
        self._sql = None
        self.user_constraint = {}

        self.defined_columns = PaletMetadata.Enrichment.getDefinedColumns(self)

        # self.report_month = datetime.strptime(report_month, '%Y%m')
        # self.start_month = datetime.strptime(start_month, '%Y%m')
        # self.end_month = datetime.strptime(end_month, '%Y%m')

        self.palet = Palet.getInstance()

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def setLoggingLevel(self, level: str):
        self.palet.logger.setLevel(level)
        return self

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
            self.palet.logger.info(f'Adding By Group {var}')
            self.by_group.append(var)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _removeByGroup(self, var):
        if var in self.by_group:
            self.palet.logger.debug(f'Removing By Group {var}')
            self.by_group.remove(var)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _addDerivedByTypeGroup(self, var):
        if var not in self.derived_by_type_group:
            self.palet.logger.debug(f'Adding By Type Group {var}')
            self.derived_by_type_group.append(var)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _addAggregateGroup(self, var):
        if var not in self.aggregate_group:
            self.palet.logger.debug(f'Adding By Aggregate Group {var}')
            self.aggregate_group.append(var)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _getAggregateGroup(self):
        self.palet.logger.debug('Forming SQL Aggregate by Groups')
        z = ""
        new_line_comma = '\n\t\t\t   ,'
        if (len(self.aggregate_group)) > 0:
            for column in self.aggregate_group:
                z += column + new_line_comma
            return f"{z}"
        else:
            return ''

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _getDerivedByTypeGroup(self):
        from palet.EnrollmentType import EnrollmentType
        from palet.CoverageType import CoverageType
        from palet.EligibilityType import EligibilityType

        z = ""
        new_line_comma = '\n\t\t\t   ,'
        if (len(self.derived_by_type_group)) > 0:
            for column in self.derived_by_type_group:

                if isinstance(column, str):
                    z += column + new_line_comma

                if str(column) == "<class 'palet.EnrollmentType.EnrollmentType'>":
                    for j in EnrollmentType.cols:
                        z += j + new_line_comma

                if str(column) == "<class 'palet.CoverageType.CoverageType'>":
                    for j in CoverageType.cols:
                        z += j + new_line_comma

                if str(column) == "<class 'palet.EligibilityType.EligibilityType'>":
                    for j in EligibilityType.cols:
                        z += j + new_line_comma

                if str(column) == "age_band":
                    z += "age_band" + new_line_comma

            return f"{z}"
        else:
            return ''

    # ---------------------------------------------------------------------------------
    #
    #   getByGroupWithAlias: This function allows our byGroup to be aliased
    #       properly for the dynamic sql generation
    #
    # ---------------------------------------------------------------------------------
    def _getByGroupWithAlias(self, alias: str = 'aa'):
        self.palet.logger.debug('Forming SQL by Groups')
        z = ""
        new_line_comma = '\n\t\t\t   ,'
        if (len(self.by_group)) > 0:
            for column in self.by_group:
                z += f"{alias}." + column + new_line_comma
            return f"{z}"
        else:
            return ''

    # ---------------------------------------------------------------------------------
    #
    #   getByGroup: This function allows our byGroup
    #       properly for the dynamic sql generation
    #
    # ---------------------------------------------------------------------------------
    def _getByGroup(self):
        self.palet.logger.debug('Forming SQL by Groups')
        z = ""
        new_line_comma = '\n\t\t\t   ,'
        if (len(self.by_group)) > 0:
            for column in self.by_group:
                z += column + new_line_comma
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
        self.palet.logger.debug('creating filter values for SQL')
        value = self.filter.get(column)
        return column + " = " + value

    # ---------------------------------------------------------------------------------p
    #
    # slice and dice here to create the proper sytax for a where clause
    #
    #
    # ---------------------------------------------------------------------------------
    def _sqlFilterWhereClause(self, alias: str, sqlloc: str = "outer"):
        self.palet.logger.debug('defining our where clause based on api calls')
        where = []

        if len(self.filter) > 0:
            for key in self.filter:
                if sqlloc == "inner" and key in PaletMetadata.Enrollment.derived_columns:
                    continue
                _in_stmt = []
                _join = ""

                # get the value(s) in case there are multiple
                values = self.filter[key]
                for val in values:
                    _in_stmt.append(f"'{val}'")

                _join = ",".join(_in_stmt)
                where.append(alias + '.' + key + ' in (' + _join + ')')

            if len(where) > 0:
                return f"{' and '.join(where)}"
            else:
                return "1=1"

        else:
            return "1=1"

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _derivedTypesWhereClause(self):
        self.palet.logger.debug('checking for user defined where clause based on api calls')
        where = []

        if len(self.user_constraint) > 0:
            for key in self.user_constraint:
                _constr = []
                _join = ""

                # get the value(s) in case there are multiple
                metaval: str = self.user_constraint[key]
                for constr in metaval:
                    for field, val in PaletMetadata.Enrollment.stack_fields.items():
                        constr = constr.replace(field, val)
                        constr = constr.format(alias=self.alias)
                    _constr.append(constr)

                _join = " or ".join(_constr)
            where.append(_join)

            return f"{' or '.join(where)}"

        else:
            return "1=1"

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _userDefinedSelect(self, sql_type: str):
        self.palet.logger.debug('checking for user defined select variabes based on api calls')

        if len(self.user_constraint) > 0:
            sel_fields = []
            alias_fmt = None
            returnval = ''

            for key, case in self.user_constraint.items():
                if sql_type == "inner":
                    for constr in case:
                        for field, val in PaletMetadata.Enrollment.common_fields.items():
                            # get the value(s) in case there are multiple
                            if constr.lower().find(field) >= 0:
                                sel_fields.append(f"{self.nested_alias}.{val}")  # TODO:
                        returnval = ',\n\t\t\t\t'.join(set(sel_fields)) + ","
                elif sql_type == "outer":
                    for constr in case:
                        for field, val in PaletMetadata.Enrollment.common_fields.items():
                            # get the value(s) in case there are multiple
                            if constr.lower().find(field) >= 0:
                                sel_fields.append(f"{self.alias}.{val}")  # TODO:
                        returnval = ',\n\t\t\t\t'.join(set(sel_fields)) + ","
                elif sql_type == "case":
                    all_fields = PaletMetadata.Enrollment.all_common_fields()
                    for cond in case:
                        for field, metaval in all_fields.items():
                            cond = cond.replace(field, metaval)
                        alias_fmt = cond.format(alias=self.alias)
                        sel_fields.append("when " + alias_fmt + " then '" + key + "'")
                    returnval = 'case ' + '\n\t\t\t\t'.join(set(sel_fields)) + ' end as defined_category,'
                else:
                    return ""

            return returnval

        else:
            return ''

    # ---------------------------------------------------------------------------------
    #
    # _chcekForMultiVarFilter
    # This is a function that is called to check for multiple value input from the user.
    # It is used internally and called during byGroup calls.
    # ---------------------------------------------------------------------------------
    def _checkForMultiVarFilter(self, values: str, separator=" "):
        self.palet.logger.debug('checking for any delimited filter values and separating them')
        return values.split(separator)

    # ---------------------------------------------------------------------------------
    #  _buildPctChangeColumn
    #  This function is used interally to create the pctChange
    #  columns based on the field names to be calculated
    #
    #
    # ---------------------------------------------------------------------------------
    def _buildPctChangeColumn(self, df: pd.DataFrame, resultColumnName: str, columnNameToCalc: str, colIntPosition, isPct: bool):
        self.palet.logger.debug('master function to create pctChange columns based on column passed')
        df[resultColumnName] = [
            round(((df[columnNameToCalc].iat[x] / df[columnNameToCalc].iat[x - colIntPosition]) - 1), 3)
            if x != 0 and df[columnNameToCalc].iat[x - 1] > 0 and df['isfirst'].iat[x] != 1
            else float('NaN')
            for x in range(len(df))]

    def _update_user_constraints(self, constraints):
        for list_constr in constraints:
            for item in list_constr:
                self.user_constraint.update(item)

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byAgeRange(self, age_range=None):
        """Filter your query by Age Range. Most top level objects inherit this function such as Enrollment, Trend, etc.
        If your object is already set by a by group this will add it as the next by group.

        Args:
            age_range: `dict, optional`: Filter a single age, range such as {'Minor': [0,17]} or
            two or more age ranges such as {'Minor': [0,17],'Young Adult': [18,25]}
            default: `none`: Defaults to pre-existing age ranges in age_grp_code

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object

        Note:
            The lowest possible age within TAF data is 0 while the highest is 125.

        Example:
            Start with an enrollment object:

            >>> api = Enrollment()

            Default Option, returns DataFrame filtered by the age ranges defined in age_grp_cd:

            >>> df = api.byAgeRange()

            Return DataFrame:

            >>> display(df.fetch())

            User defined option, analyst enters a dictionary of labels with minimum and maximum ages:

            >>> df = api.byAgeRange({'Teenager': [13,19],'Twenties': [20,29],'Thirties': [30,39]})

            Return DataFrame:

            >>> display(df.fetch())

        """

        self.palet.logger.info('adding byAgeRange to aggregate by Group')

        if age_range is not None:
            self._removeByGroup(PaletMetadata.Enrollment.identity.ageGroup)
            self.age_band = age_range
            self._addDerivedByTypeGroup(PaletMetadata.Enrollment.identity.age_band)
            self._addAggregateGroup(PaletMetadata.Enrollment.identity.age_band)

        else:
            self._addByGroup(PaletMetadata.Enrollment.identity.ageGroup)

        return self

    # ---------------------------------------------------------------------------------
    #
    # Likely will be removed - consolidated to be included in byAgeRange()
    #
    # ---------------------------------------------------------------------------------
    # def byAgeGroup(self, age_group=None):

    #     self.palet.logger.info('adding byAgeGroup to by Group')

    #     self._addByGroup(PaletMetadata.Enrollment.identity.ageGroup)

    #     if age_group is not None:
    #         self.filter.update({PaletMetadata.Enrollment.identity.ageGroup: age_group})

    #     return self

    # ---------------------------------------------------------------------------------
    #
    # add any byEthnicity values
    #
    #
    # ---------------------------------------------------------------------------------
    def byRaceEthnicity(self, ethnicity=None):
        """Filter your query by Race. Most top level objects inherit this function such as Enrollment, Trend, etc.
        If your object is already set by a by group this will add it as the next by group. The values in the race column
        correspond to the to the codes and races from race_ethncty_flag in TAF. See race_ethncty_flag in PaletMetadata.

        Args:
            ethnicity: `str, optional`: Filter a single race by entering the corresponding code from race_ethncty_flag
            default: `none`: Filter by all races in race_ethncty_flag

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object

        Note:
            The codes and corresponding races of race_ethncty_flag can be found in PaletMetadata.

        Example:
            Start with a Paletable object and use the default option to filter query by race and ethnicity:

            >>> api = Enrollment().byRaceEthnicity()

            Return DataFrame:

            >>> display(api.fetch())

            Focus in on one race, in this example Asian beneficiaries:

            >>> api = Enrollment().byRaceEthnicity('3')

            Return DataFrame:

            >>> display(api.fetch())

        """

        self.palet.logger.info('adding byRaceEthnicity to by Group')
        self._addByGroup(PaletMetadata.Enrollment.raceEthnicity.race)

        if ethnicity is not None:
            self.filter.update({PaletMetadata.Enrollment.raceEthnicity.race: ethnicity})

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byRaceEthnicityExpanded(self, ethnicity=None):
        """Filter your query by Race Ethnicity Expanded. Most top level objects inherit this function such as Enrollment, Trend, etc.
        If your object is already set by a by group this will add it as the next by group. The values in the raceExpanded column
        correspond to the to the codes and races from race_ethncty_exp_flag in TAF. See race_ethncty_exp_flag in PaletMetadata.

        Args:
            ethnicity: `str, optional`: Filter a single race by entering the corresponding code from race_ethncty_exp_flag
            default: `none`: Filter by all races in race_ethncty_exp_flag

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object

        Note:
            The codes and corresponding races of race_ethncty_exp_flag can be found in PaletMetadata.

        Example:
            Start with a Paletable object and use the default option to filter query by race and ethnicity:

            >>> api = Enrollment().byRaceEthnicityExpanded()

            Return DataFrame:

            >>> display(api.fetch())

            Focus in on one race, in this example Native American beneficiaries:

            >>> api = Enrollment().byRaceEthnicityExpanded('3')

            Return DataFrame:

            >>> display(api.fetch())

        """

        self.palet.logger.info('adding byRaceEthnicityExpanded to by Group')
        self._addByGroup(PaletMetadata.Enrollment.raceEthnicity.raceExpanded)

        if ethnicity is not None:
            self.filter.update({PaletMetadata.Enrollment.raceEthnicity.raceExpanded: ethnicity})

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byEthnicity(self, ethnicity=None):
        """Filter your query by Ethnicity. Most top level objects inherit this function such as Enrollment, Trend, etc.
        If your object is already set by a by group this will add it as the next by group. The values in the ethnicity column
        correspond to the to the codes and races from ethncty_cd in TAF. See ethncty_cd in PaletMetadata.

        Args:
            ethnicity: `str, optional`: Filter a single ethnicity by entering the corresponding code from ethncty_cd
            default: `none`: Filter by all ethnicities in ethncty_cd

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object

        Note:
            The codes and corresponding races of race_ethncty_flag can be found in PaletMetadata.

        Example:
            Start with a Paletable object and use the default option to filter query by ethnicity:

            >>> api = Enrollment().byEthnicity()

            Return DataFrame:

            >>> display(api.fetch())

            Focus in on one race, in this example Cuban beneficiaries:

            >>> api = Enrollment().byEthnicity('3')

            Return DataFrame:

            >>> display(api.fetch())

        """

        self.palet.logger.info('adding byEthnicity to by Group')
        self._addByGroup(PaletMetadata.Enrollment.raceEthnicity.ethnicity)

        if ethnicity is not None:
            self.filter.update({PaletMetadata.Enrollment.raceEthnicity.ethnicity: ethnicity})

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byGender(self, gender=None):
        """Filter your query by Gender. Most top level objects inherit this function such as Enrollment, Trend, etc.
        If your object is already set by a by group this will add it as the next by group. This by group is filtering
        using the gndr_cd column in the TAF data.

        Args:
            gender: `str, optional`: Filter by a single gender using the corresponding code from gndr_cd
            default: `none`: Defaults to filtering by all three options: -1 (null), F (female), M (male).

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object

        Example:
            Create a Paletable object:

            >>> api = Eligibility()

            Use byGender() to filter the DataFrame by gender:

            >>> df = api.byGender().fetch()

            Return the DataFrame:

            >>> display(df)

            Pivot to focus only on male beneficiaries:

            >>> df = api.byGender('M').fetch()

            Return the DataFrame:

            >>> display(df)

        """

        self.palet.logger.info('adding byGender to by Group')

        self._addByGroup(PaletMetadata.Enrollment.identity.gender)

        if gender is not None:
            self.filter.update({PaletMetadata.Enrollment.identity.gender: gender})

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byState(self, state_cds: list = None):
        """Filter your query by State with total enrollment. Most top level objects inherit this function such as Enrollment, Trend, etc.
            If your object is already set by a by group this will add it as the next by group.

        Args:
            state_fips:`list, (optional)`: Filter by a single State or multiple states using FIPS code. Note brackets around state or states is required.
            default: `none`: Filter the Paletable object by all states and territories.

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object

        Example:
            Create a Paletable object for enrollment filtered by state:

            >>> api = Enrollment().byState()

            Return the object as DataFrame:

            >>> display(api.fetch())

            Focus in one state, for this example Alabama:

            >>> api = Enrollment().byState(['01'])

            or

            >>> api = Enrollment().byState(['NC'])

            Return the object as DataFrame:

            >>> display(api.fetch())

        """
        _states_ = []

        self.palet.logger.info('adding byState to the by Group')
        if self.timeunit not in ('full', 'year', 'partial'):
            self.timeunit = 'month'

        self._addByGroup(PaletMetadata.Enrollment.locale.submittingState)

        if state_cds is not None and type(state_cds) is list:
            if type(state_cds) is not int:
                lkup = self.palet.st_fips
                for state in state_cds:
                    fips = lkup[lkup['STABBREV'] == state]['FIPS']
                    state_fips = fips.iloc[0]
                    _states_.append(state_fips)
            else:
                _states_.extend(state_cds)
            self.filter.update({PaletMetadata.Enrollment.locale.submittingState: _states_})
        else:
            PaletMetadata.Enrichment._checkForHelperMsg(state_cds, list, "['NC','NY','FL']")

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byCoverageType(self, constraint: list = None):
        """Filter your query by coverage type. Most top level objects inherit this function such as Enrollment, Trend, etc.
            If your object is already set by a by group this will add it as the next by group. Coverage type codes and values
            correspond to coverage_type in PaletMetadata.

        Args:
            type:`list, (optional)`: Filter by an individual coverage type or multiple coverage types using coverage code. Brackets are required.
            default: `none`: Filter by all available coverage types.

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object

        Note:
            The :class:`CoverageType` class is automatically imported when this by group is called.

        Example:
            Create Paletable object:

            >>> api = Enrollment().byCoverageType()

            Return Paletable object as a DataFrame:

            >>> display(api.fetch())

        """
        from palet.CoverageType import CoverageType

        self.palet.logger.info('adding CoverageType to the by Group')
        self.derived_by_type_group.append(CoverageType)

        if constraint is not None:
            PaletMetadata.Enrichment._checkForHelperMsg(constraint, list, "['01', '02', '03']")
            self.filter_by_type.update({CoverageType: constraint})

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byEnrollmentType(self, constraint: list = None):
        """Filter your query by enrollment type. Most top level objects inherit this function such as Eligibility, Trend, etc.
        If your object is already set by a by group this will add it as the next by group. Enrollment type codes and values
        correspond to chip_cd in PaletMetadata.

        Args:
            type:`list, (optional)`: Filter by an individual enrollment type or multiple enrollment types using enrollment type code. Brackets are required.
            default: `none`: Filter by all available enrollment types

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object

        Example:
            Create Paletable object:

            >>> api = Enrollment().byEnrollmentType()

            Return Paletable object as a DataFrame:

            >>> display(api.fetch())

        """
        from palet.EnrollmentType import EnrollmentType

        self.palet.logger.info('adding byEnrollmentType to the by Group')
        self.derived_by_type_group.append(EnrollmentType)

        if constraint is not None:
            PaletMetadata.Enrichment._checkForHelperMsg(constraint, list, "['1', '2', '3']")
            self.filter_by_type.update({EnrollmentType: constraint})

        return self

    # ---------------------------------------------------------------------------------
    #
    # Stub method for byEligibilityType user entries overloading
    # ---------------------------------------------------------------------------------
    @overload
    def byEligibilityType(self, constraint: dict = None) -> None:
        """Filter your query by enrollment type. Most top level objects inherit this function such as Eligibility, Trend, etc.
        If your object is already set by a by group this will add it as the next by group. Enrollment type codes and values
        correspond to chip_cd in PaletMetadata.

        Args:
            type:`dict, (optional)`: Filter by one or more user specified constraints in dict format.

            default: `none`: Filter by all available enrollment types

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object

        Example:

            Filter by special Eligibility Types:

            >>> api = Enrollment().byEligibilityType([EligibilityType.BlindAndDisabled, EligibilityType.Aged])

            or

            Filter by user defined group(s)

            >>> api = Enrollment().byEligibilityType(constraint={"Pregnant": ["(bb.age_num <= 59 or bb.age_num is null) and (bb.GNDR_CD='F'
                                                                                or bb.GNDR_CD is null) and bb.eligibility_type
                                                                                in ('67','68','05','53')"]})

        Filter your query by enrollment type. Most top level objects inherit this function such as Eligibility, Trend, etc.
        If your object is already set by a by group this will add it as the next by group. Enrollment type codes and values
        correspond to chip_cd in PaletMetadata.

        Args:
            type:`list, (optional)`: Filter by one or more eligibilty types using eligibility type code or special groups within EligilibilityType.
                                     Brackets are required.

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object

        Example:
            Filter by user defined group(s)

            >>> api = Enrollment().byEligibilityType([EligibilityType.BlindAndDisabled, EligibilityType.Aged])

            or

            >>> api = Enrollment().byEligibilityType(['01', '02', '03'])

        Args:
            default: `none`:No filter / show all available enrollment types

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object

        Example:
            Create Paletable object:

            >>> api = Enrollment().byEligibilityType()

            Return Paletable object as a DataFrame:

            >>> display(api.fetch())"""
        ...

    @overload
    def byEligibilityType(self, constraint: list = None) -> None:
        ...

    @overload
    def byEligibilityType(self) -> None:
        ...

    def byEligibilityType(self, constraint=None):
        from palet.EligibilityType import EligibilityType

        self.palet.logger.info('adding EligibilityType to the by Group')
        self.derived_by_type_group.append(EligibilityType)

        if constraint is not None:

            if isinstance(constraint, list):
                PaletMetadata.Enrichment._checkForHelperMsg(constraint, list, "['01', '02', '03']")
                self.palet.logger.info("Special Types were specified. The query will use these types and user defined constaints will be ignored.")
                self.filter_by_type.update({EligibilityType: constraint})
                if type(constraint[0]) is tuple:
                    self._update_user_constraints(constraint)
            elif isinstance(constraint, dict):
                self.palet.logger.info("User Defined Constraints were specified. The query will use these constraints and special types will be ignored.")
                self.user_constraint.update(constraint)

                for _field, val in PaletMetadata.Enrollment.common_fields.items():
                    # get the value(s) in case there are multiple
                    self.by_group.append(val)

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    # This function is just returning the straight data from the table
    def byIncomeBracket(self, bracket: list = None):
        """Filter your query by income bracket. Most top level objects inherit this function such as Enrollment, Trend, etc.
            If your object is already set by a by group this will add it as the next by group.

        Args
        ----
            bracket:`str, (optional)`: Filter by incm_cd, can be individual, range, or multiple. See examples below.
            default: `none`: Filter data by all possible incm_cd's.

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

        self.palet.logger.info('adding byIncomeBracket to the by Group')
        PaletMetadata.Enrichment._checkForHelperMsg(bracket, list, "byIncomeBracket(['03', '05']) or byIncomeBracket(['01'])")
        self._addByGroup(PaletMetadata.Enrollment.identity.income)
        if bracket is not None:
            self.filter.update({PaletMetadata.Enrollment.identity.income: bracket})

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byYear(self, year: list = None):
        """Filter your query by Year. Most top level objects inherit this function such as Enrollment, Trend, etc.

        Args:
            year:`list, (optional)`: Filter by year using the year or years in numerical format. Input is list, brackets are required even when looking at one year.
            default: `none`: Filter object by all states and territories available.

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object

        Example:
            Create Paletable object filtering by all available years:

            >>> api = Enrollment().byYear()

            Return object as a DataFrame:

            >>> display(api.fetch())

            Create Paletable object filtering by 2019 and 2020:

            >>> api = Enrollment().byYear([2019, 2020])

            Return object as a DataFrame:

            >>> display(api.fetch())

        """

        self.palet.logger.info('adding byYear to the by Group')

        self.timeunit = 'year'

        if year is not None:
            self.date_dimension.years = year

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byMonth(self, month: list = None):
        """Filter your query by Month. Most top level objects inherit this function such as Enrollment, Trend, etc.

        Args:
            month:`list, (optional)`: Filter by a specific month or months using the month(s) in numerical format. Input is list, brackets are required even when looking at one month.
            default: `none`: Filter object by all available months.

        Returns:
            Spark DataFrame: :class:`Paletable`: returns the updated object

        Example:
            Create Paletable object filtering by all available months:

            >>> api = Enrollment().byMonth()

            Return object as a DataFrame:

            >>> display(api.fetch())

            Create Paletable object focusing only on the month of December in the given time period:

            >>> api = Enrollment().byMonth(12)

        """
        # TODO: Fix best way to allow for multiple month and year

        self.palet.logger.info('adding byMonth to the by Group')
        self.timeunit = 'month'

        if month is not None:
            self._outersql.update({"month": month})
            self.date_dimension.months = month

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _selectTimeunit(self, alias: str = None):
        a = ''
        if alias is not None:
            a = f"{alias}."

        if self.timeunit in ('year', 'partial_year'):
            return f"{a}de_fil_dt as year,"
        elif self.timeunit in ('month', 'full', 'partial'):
            return f"""
                {a}de_fil_dt as year,
                {a}month,
                """

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _groupTimeunit(self, alias: str = None):
        a = ''
        if alias is not None:
            a = f"{alias}."

        if self.timeunit in ('year', 'partial_year'):
            return f"{a}de_fil_dt"
        elif self.timeunit in ('month', 'full', 'partial'):
            return f"""
                {a}de_fil_dt,
                {a}month
                """

    def _sumByTypeScenario(self, df: pd.DataFrame):
        df['num_types_per_year'] = df['SUBMTG_STATE_CD'].apply(lambda x: str(x).zfill(2))
        # if str(column) == "<class 'palet.EnrollmentType.EnrollmentType'>":

        # df.groupby(by=['STABBREV', timeunit]).sum().reset_index()

        pass

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

        self.postprocesses = []

        return self.sql

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def fetch(self, asPandas: bool = True):
        """Call this method at the end of an object when you are ready for results.

        This converts the Spark DataFrame generated by a Paletable object into a Pandas DataFraem. This can be leveraged with display()
        to quickly pivot results. fetch() also initiates enrichment and decoration such as adding more readable columns and including
        percentage changes.

        Args:
            None: No input required

        Returns:
            Pandas DataFrame: Executes your query and returns a Pandas DataFrame.

        Example:
            Create object for enrollment by state and year

            >>> api = Enrollment().byState()

            Return Spark DataFrame:

            >>> api.fetch

            Lever display() to pivot from yearly to monthly

            >>> display(api.byMonth().fetch())
        """
        from pyspark.sql.types import StringType, DecimalType, IntegerType, LongType, DoubleType
        from pyspark.sql.utils import AnalysisException, ParseException
        from pyspark.sql import SparkSession

        session = SparkSession.getActiveSession()

        self._sql = self.sql(isFetch=True)

        self.palet.logger.debug('Fetching data - \n' + self._sql)

        try:
            sparkDF = session.sql(self._sql)
        except ParseException as pe:
            self.palet.logger.fatal("There was a " + pe.cause + " in the sql query. Please review the syntax. \
                                    To view the query being executed try 'print (api.sql())'")
            return
        except AnalysisException as ae:
            self.palet.logger.fatal("There was a " + ae.cause + " in the sql query. Please review the syntax. \
                                    To view the query being executed try 'print (api.sql())'")
            return

        self._sql = None

        if sparkDF is not None:

            for column in self.by_group:
                sparkDF = sparkDF.withColumn(column, sparkDF[column].cast(StringType()))

            if 'mdcd_total_amount' in sparkDF.columns:
                sparkDF = sparkDF.withColumn('mdcd_total_amount', sparkDF['mdcd_total_amount'].cast(DoubleType()))

            if 'chip_total_amount' in sparkDF.columns:
                sparkDF = sparkDF.withColumn('chip_total_amount', sparkDF['chip_total_amount'].cast(DoubleType()))

            if 'mdcd_pmpm' in sparkDF.columns:
                sparkDF = sparkDF.withColumn('mdcd_pmpm', sparkDF['mdcd_pmpm'].cast(DoubleType()))

            if 'chip_pmpm' in sparkDF.columns:
                sparkDF = sparkDF.withColumn('chip_pmpm', sparkDF['chip_pmpm'].cast(DoubleType()))

            if 'mdcd_util' in sparkDF.columns:
                sparkDF = sparkDF.withColumn('mdcd_util', sparkDF['mdcd_util'].cast(DoubleType()))

            if 'chip_util' in sparkDF.columns:
                sparkDF = sparkDF.withColumn('chip_util', sparkDF['chip_util'].cast(DoubleType()))

            if 'mdcd_cost' in sparkDF.columns:
                sparkDF = sparkDF.withColumn('mdcd_cost', sparkDF['mdcd_cost'].cast(DoubleType()))

            if 'chip_cost' in sparkDF.columns:
                sparkDF = sparkDF.withColumn('chip_cost', sparkDF['chip_cost'].cast(DoubleType()))

            if asPandas:
                df = sparkDF.toPandas()

                if df.empty is False:

                    for column in self.by_group:
                        df[column] = df[column].astype(pd.StringDtype())
                        df[column].fillna('-1', inplace=True)

                    # perform data post process
                    #  self.palet.logger.debug('Beginning call to run post-processes')
                    #  for pp in self.postprocesses:
                    #      df = pp(df)

                    # perform data enrichments
                    self.palet.logger.debug('Beginning call to run post-processes')
                    for column in self.defined_columns:
                        if column in df.columns:
                            self.palet.logger.debug("Calling post-process " + column)
                            col = self.defined_columns[column]
                            df = col(df)

                    return df

        return sparkDF

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def log(self, viewname: str, sql=''):
        """
        This attribute allows you to print out the sql of a specific view within databricks or a database
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
