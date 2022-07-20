"""
The Enrollment module allows CMS analysts to view enrollment. This module can be levereged with the Paletable module
to apply specific filters. Doing so, analysts can view enrollment by state, income bracket, age, etc. This module
uses the pandas library and elements of the pyspark library. Note the Paletable module is imported here as well. As such,
the Enrollment module inherits from the Paletable module.
"""

from palet.Diagnoses import Diagnoses
from palet.Palet import Palet
from palet.PaletMetadata import PaletMetadata
from palet.Paletable import Paletable
import pandas as pd
from datetime import date


class Enrollment(Paletable):
    """
    The class within the PALET library for viewing enrollment. This class is used to view enrollment for Medicaid and CHIP.
    Attributes inherited from the Paletable class can be used to apply and_filters for enrollee age, ehtnicity, gender, state, income, etc.

    Enrollment counts are the sum of the unique beneficiaries enrolled at least one day in a given month or year.

    Note:
        If the Enrollment class is called without a by group, it defaults to by month.

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

        User defined run ids:

        >>> api = Enrollment([6278, 6280])

        Specifying run ids and switching context

        or

        switching context by Parameter naming

        >>> api = Eligibility([6278, 6280], api) or

        >>> api = Eligibility([], api)

        >>> api = Eligibility(paletable=api)

        You may also request FULL or PARTIAL month enrollments.

        >>> api = Enrollment(period='full')

        or

        >>> api = Enrollment(period='partial')

    Args:
        list: List of defined run ids you wish to use. Not required, defaults to list of latest run ids.
        Paletable: No input required, defaults to None.

    Returns:
        Spark DataFrame: DataFrame with counts for enrollment and precentage changes from previous period.

    Methods:
        usingRundIds(): Specify the run ids you would like to query. See :meth:`~DateDimension.DateDimension.usingRunIds`.
        displayCurrentRunIds(): DEPRECATED. See :class:`DateDimension`.
        byAgeRange(): Filter your query by Age Range. See :meth:`~Paletable.Paletable.byAgeRange`.
        byRaceEthnicity(): Filter your query by Race. See :meth:`~Paletable.Paletable.byRaceEthnicity`.
        byRaceEthnicityExpanded(): Filter your query by Race (expanded options). See :meth:`~Paletable.Paletable.byRaceEthnicityExpanded`.
        byEthnicity(): Filter your query by Ethnicity. See :meth:`~Paletable.Paletable.byEthnicity`.
        byGender(): Filter your query by Gender. See :meth:`~Paletable.Paletable.byGender`.
        byState(): Filter your query by State. See :meth:`~Paletable.Paletable.byState`.
        byCoverageType(): Filter your query by Coverage Type. See :meth:`~Paletable.Paletable.byCoverageType`.
        byEnrollmentType(): Filter your query by Enrollment Type. See :meth:`~Paletable.Paletable.byEnrollmentType`.
        byEligibilityType(): Filter your query by Eligibility Type. See :meth:`~Paletable.Paletable.byEligibilityType`.
        byMedicaidOnly(): Filter your query to only look at Medicaid enrollment :meth:`~Paletable.Paletable.byMedicaidOnly`.
        byIncomeBracket(): Filter your query by Income Bracket. See :meth:`~Paletable.Paletable.byIncomeBracket`.
        byYear(): Filter your query by Year. See :meth:`~Paletable.Paletable.byYear`.
        byMonth(): Filter your query by Month. See :meth:`~Paletable.Paletable.byMonth`.
        fetch(): Call this function when you are ready to return results. See :meth:`~Paletable.Paletable.fetch`.

    Note:
        The above attributes are inherited from the :class:`Paletable` class with the exception of :meth:`~DateDimension.DateDimension.usingRunIds`.
        See :class:`DateDimension` Attributes directly from the Enrollment class can be seen below.

    """

    # -----------------------------------------------------------------------
    # Initialize the Enrollment API
    # -----------------------------------------------------------------------
    def __init__(self, runIds: list = None, asOf: date = None, paletable: Paletable = None, period: str = "month"):
        # print('Initializing Enrollment API')
        super().__init__(asOf=asOf, runIds=runIds)

        if (paletable is not None):
            self.by_group = paletable.by_group
            self.derived_by_type_group = paletable.derived_by_type_group
            self.aggregate_group = paletable.aggregate_group
            self.filter = paletable.filter
            self.defined_columns = paletable.defined_columns

        self.palet = Palet.getInstance()
        self.palet.clearAliasStack()

        self.timeunit = period

        self._marker_cache = []
        self._groupby_cache = []

        self._outersql = {}
        self._sql = None
        self.user_constraint = {}

        self.alias = 'bb'
        self.nested_alias = 'aa'

        self.palet.logger.debug('Initializing Enrollment API')

    # ---------------------------------------------------------------------------------
    # timeunit class
    # Create the proper summary columns based on the by timeunit selected.
    # Use the stack SQL function to create columns
    #
    # ---------------------------------------------------------------------------------
    class timeunit():
        """
        The timeunit class is a subclass within the Enrollment. It is composed of two dictionaries: breakdown & cull.
        This class specifies how counts for enrollment are calculated. When looking at enrollment by year, enrollment
        refers to beneficiaries who are enrolled at least one day in a given year. When looking at enrollment by
        month, enrollment refers to beneficiaries who are enrolled at least one day in a given month.

        Breakdown - Provides the sum of all beneficiaries enrolled within the time period or periods specified.

        Cull - Provides the individual beneficiaries enrolled within the time period or periods specified.

        Available Time Units:
            In Year (year) - Beneficiaries enrolled at least one day in a given year

            In Month (month) - Beneficiaries enrolled at least one day in a given month

            Full Month (full) - Beneficiaries enrolled in n days of a given month, where n is the total number of days in said month

            Partial Month (partial) - Beneficiaries enrolled in 1 to n-1 days of a given month, where n is the total number of days in said month

        Example:
            Specifying the time unit using .timeunit:

            >>> api = Enrollment()

            >>> api.timeunit = 'year'

            >>> api.timeunit = 'month'

            >>> api.timeunit = 'full'

            >>> api.timeunit = 'partial'

            Specifying the time unit using Enrollment()'s parameters:

            >>> api = Enrollment('year')

            >>> api = Enrollment('month')

            >>> api = Enrollment('partial')

            >>> api = Enrollment('full')

        Note:
            This class affects both Medicaid & CHIP Enrollment.

        """

        breakdown = {
            'year': f"""
                'In Year' as counter,
                stack(1,
                    1, { {13} }
                        sum(case when aa.mdcd_enrlmt_days_yr > 0 then 1 else 0 end),
                        sum(case when aa.chip_enrlmt_days_yr > 0 then 1 else 0 end)
                    ) as (year, { {12} } mdcd_enrollment, chip_enrollment)""",
            # 'partial_year': f"""
            #     'Partial Year' as counter,
            #     stack(1,
            #         1, { {13} }
            #             sum(case when aa.de_fil_dt % 4 = 0 or aa.de_fil_dt % 100 = 0
            #                     and aa.de_fil_dt % 400 = 0
            #                         and aa.mdcd_enrlmt_days_yr between 1 and 366
            #                 or aa.de_fil_dt % 4 != 0 or aa.de_fil_dt % 100 != 0
            #                     and aa.de_fil_dt % 400 != 0
            #                         and aa.mdcd_enrlmt_days_yr between 1 and 365 then 1 else 0 end),
            #             sum(case when aa.de_fil_dt % 4 = 0 or aa.de_fil_dt % 100 = 0
            #                     and aa.de_fil_dt % 400 = 0
            #                         and aa.chip_enrlmt_days_yr between 1 and 366
            #                 or aa.de_fil_dt % 4 != 0 or aa.de_fil_dt % 100 != 0
            #                     and aa.de_fil_dt % 400 != 0
            #                         and aa.chip_enrlmt_days_yr between 1 and 365 then 1 else 0 end)
            #     ) as (year, { {12} } mdcd_enrollment, chip_enrollment)""",
            'month': f"""
                {PaletMetadata.Enrollment.sqlstmts.enroll_count_stmt()}
                """,

            'full': f"""
                {PaletMetadata.Enrollment.sqlstmts.enroll_count_full_stmt()}
                """,

            'partial': f"""
                {PaletMetadata.Enrollment.sqlstmts.enroll_count_partial_stmt()}
                """
        }

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _getDerivedTypeSelections(self):
        derived_types = []

        # if (len(self.derived_by_group)) > 0 and self.timeunit != 'year':
        if (len(self.derived_by_type_group)) > 0:
            for bytype in self.derived_by_type_group:
                derived_types.append(f"{self.alias}.{bytype.alias}")

            return ",\n".join(derived_types) + ","

        return ''

    # ---------------------------------------------------------------------------------
    # Used in _percentChange to enure the derived by groups for mdcd_pct and chip_pct
    # are included in the sort and order commands.
    # ---------------------------------------------------------------------------------
    def _getDerivedTypePctSort(self):
        from palet.EnrollmentType import EnrollmentType
        from palet.CoverageType import CoverageType
        from palet.EligibilityType import EligibilityType

        _type_groups = []

        # if (len(self.derived_by_group)) > 0 and self.timeunit != 'year':
        if (len(self.derived_by_type_group)) > 0:
            for column in self.derived_by_type_group:
                if str(column) == "<class 'palet.EnrollmentType.EnrollmentType'>":
                    _type_groups.append(EnrollmentType.alias)

                elif str(column) == "<class 'palet.CoverageType.CoverageType'>":
                    _type_groups.append(CoverageType.alias)

                elif str(column) == "<class 'palet.EligibilityType.EligibilityType'>":
                    _type_groups.append(EligibilityType.alias)

                else:
                    _type_groups.append(column)

            return _type_groups

        return []

    # ---------------------------------------------------------------------------------
    #
    #
    # Used in _percentChange to enure marked columns for mdcd_pct and chip_pct are included
    # in the sort and order commands.
    # ---------------------------------------------------------------------------------
    def _getMarkerPctSort(self):
        indicators = []
        for key, val in self.markers.items():
            indicators.append(key)
        return indicators

    # ---------------------------------------------------------------------------------
    #
    #
    # _getTimeunitBreakdown
    # This function is used to dynamically generate the SQL statement by returning the
    # selected timeunit. e.g. byMonth() or byYear()
    #
    #
    # ---------------------------------------------------------------------------------
    def _getTimeUnitBreakdown(self):
        # from palet.EnrollmentType import EnrollmentType
        # from palet.CoverageType import CoverageType
        # from palet.EligibilityType import EligibilityType

        breakdown = Enrollment.timeunit.breakdown[self.timeunit]

        series_00 = []
        series_01 = []
        series_02 = []
        series_03 = []
        series_04 = []
        series_05 = []
        series_06 = []
        series_07 = []
        series_08 = []
        series_09 = []
        series_10 = []
        series_11 = []
        aliases = []
        aggregates = []

        if (len(self.derived_by_type_group)) > 0:
            for column in self.derived_by_type_group:

                # <class 'palet.EnrollmentType.EnrollmentType'>
                # <class 'palet.EligibilityType.EligibilityType'>
                # <class 'palet.CoverageType.CoverageType'>
                series_00.append('aa.{0}'.format(column.cols[0]))
                series_01.append('aa.{0}'.format(column.cols[1]))
                series_02.append('aa.{0}'.format(column.cols[2]))
                series_03.append('aa.{0}'.format(column.cols[3]))
                series_04.append('aa.{0}'.format(column.cols[4]))
                series_05.append('aa.{0}'.format(column.cols[5]))
                series_06.append('aa.{0}'.format(column.cols[6]))
                series_07.append('aa.{0}'.format(column.cols[7]))
                series_08.append('aa.{0}'.format(column.cols[8]))
                series_09.append('aa.{0}'.format(column.cols[9]))
                series_10.append('aa.{0}'.format(column.cols[10]))
                series_11.append('aa.{0}'.format(column.cols[11]))

                aliases.append(column.alias)
                aggregates.append(column.aggregate('aa'))

            z = breakdown.format(
                ', '.join(series_00) + ',',
                ', '.join(series_01) + ',',
                ', '.join(series_02) + ',',
                ', '.join(series_03) + ',',
                ', '.join(series_04) + ',',
                ', '.join(series_05) + ',',
                ', '.join(series_06) + ',',
                ', '.join(series_07) + ',',
                ', '.join(series_08) + ',',
                ', '.join(series_09) + ',',
                ', '.join(series_10) + ',',
                ', '.join(series_11) + ',',
                ', '.join(aliases) + ',',
                ', '.join(aggregates) + ',')

            return z

        return breakdown.format('', '', '', '', '', '', '', '', '', '', '', '', '', '')

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _getOuterSQLFilter(self, filters: dict):
        filter = filters[self.timeunit]
        _stmt_list = []

        if len(self._outersql) > 0:
            for key in self._outersql:
                vals = self._outersql[key]
                if vals is not None:
                    _str = ','.join(f"'{x}'" for x in vals)
                    _stmt_list.append(filter.format(key, _str))

            _outer = " AND ".join(_stmt_list)
            return _outer
        else:
            return "1=1"

    # ---------------------------------------------------------------------------------
    # _percentChange protected/private method that is called by each fetch() call
    # to calculate the % change columns. Each Paletable class should override this
    # and create it's own logic.
    # ---------------------------------------------------------------------------------
    def _percentChange(self, df: pd.DataFrame):
        self.palet.logger.debug('Percent Change')

        # df['year'] = df['de_fil_dt']

        # if self.timeunit != 'full' and self.timeunit != 'year' and self.timeunit != 'partial':

        if self.timeunit != 'year':
            # Month-over-Month
            if (len(self.by_group)) > 0 and (len(self.derived_by_type_group)) > 0:
                df = df.sort_values(by=self.by_group + self._getDerivedTypePctSort() + ['year', 'month'], ascending=True)
                df.loc[df.groupby(self.by_group + self._getDerivedTypePctSort()).apply(pd.DataFrame.first_valid_index), 'isfirst'] = 1
            elif (len(self.by_group)) > 0:
                df = df.sort_values(by=self.by_group + ['year', 'month'], ascending=True)
                df.loc[df.groupby(self.by_group).apply(pd.DataFrame.first_valid_index), 'isfirst'] = 1
            elif (len(self.derived_by_type_group)) > 0:
                df = df.sort_values(by=self._getDerivedTypePctSort() + ['year', 'month'], ascending=True)
                df.loc[df.groupby(self._getDerivedTypePctSort()).apply(pd.DataFrame.first_valid_index), 'isfirst'] = 1
            else:
                df = df.sort_values(by=['year', 'month'], ascending=True)
                df['isfirst'] = 0

            self._buildPctChangeColumn(df, 'mdcd_pct_mom', 'mdcd_enrollment', 1, False)
            self._buildPctChangeColumn(df, 'chip_pct_mom', 'chip_enrollment', 1, False)

            # Year-over-Year
            if (len(self.by_group)) > 0 and (len(self.derived_by_type_group)) > 0:
                df = df.sort_values(by=self.by_group + self._getDerivedTypePctSort() + ['month', 'year'], ascending=True)
                df.loc[df.groupby(self.by_group + self._getDerivedTypePctSort() + ['month']).apply(pd.DataFrame.first_valid_index), 'isfirst'] = 1
            elif (len(self.by_group)) > 0:
                df = df.sort_values(by=self.by_group + ['month', 'year'], ascending=True)
                df.loc[df.groupby(self.by_group + ['month']).apply(pd.DataFrame.first_valid_index), 'isfirst'] = 1
            elif (len(self.derived_by_type_group)) > 0:
                df = df.sort_values(by=self._getDerivedTypePctSort() + ['month', 'year'], ascending=True)
                df.loc[df.groupby(self._getDerivedTypePctSort() + ['month']).apply(pd.DataFrame.first_valid_index), 'isfirst'] = 1
            else:
                df = df.sort_values(by=['month', 'year'], ascending=True)
                df.loc[df.groupby(['month']).apply(pd.DataFrame.first_valid_index), 'isfirst'] = 1

            self._buildPctChangeColumn(df, 'mdcd_pct_yoy', 'mdcd_enrollment', 1, False)
            self._buildPctChangeColumn(df, 'chip_pct_yoy', 'chip_enrollment', 1, False)

            # Re-sort Chronologically
            if (len(self.derived_by_type_group)) > 0:
                df = df.sort_values(by=self.by_group + self._getDerivedTypePctSort() + ['year', 'month'], ascending=True)
            else:
                df = df.sort_values(by=self.by_group + ['year', 'month'], ascending=True)

        elif self.timeunit == 'year':

            # Year-over-Year
            if (len(self.derived_by_type_group)) > 0 and (len(self.by_group)) > 0:
                df = df.sort_values(by=self.by_group + self._getDerivedTypePctSort() + ['year'], ascending=True)
                df.loc[df.groupby(self.by_group + self._getDerivedTypePctSort()).apply(pd.DataFrame.first_valid_index), 'isfirst'] = 1
            elif (len(self.by_group)) > 0:
                df = df.sort_values(by=self.by_group + ['year'], ascending=True)
                df.loc[df.groupby(self.by_group).apply(pd.DataFrame.first_valid_index), 'isfirst'] = 1
            elif (len(self.derived_by_type_group)) > 0:
                df = df.sort_values(by=self._getDerivedTypePctSort() + ['year'], ascending=True)
                df.loc[df.groupby(self._getDerivedTypePctSort()).apply(pd.DataFrame.first_valid_index), 'isfirst'] = 1
            else:
                df = df.sort_values(by=['year'], ascending=True)
                df['isfirst'] = 0

            self._buildPctChangeColumn(df, 'mdcd_pct_yoy', 'mdcd_enrollment', 1, False)
            self._buildPctChangeColumn(df, 'chip_pct_yoy', 'chip_enrollment', 1, False)

        return df

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def byServiceCategory(self, services: list = [], lookback=6):
        """Filter your query by service category. Most top level objects inherit this function such as Enrollment, Trend, etc.
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
        >>> Enrollment.byServiceCategory('iph')
        """
        from palet.ServiceCategory import ServiceCategory

        if len(services) == 0:
            services.append(ServiceCategory.inpatient)
            services.append(ServiceCategory.long_term)
            services.append(ServiceCategory.other_services)
            services.append(ServiceCategory.prescription)

        return self.having(ServiceCategory.within(service_categories=services, diagnoses=[], lookback=lookback))

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def mark(self, condition: Diagnoses, marker: str):
        """
        The mark function appends a condition column to a dataframe that was filtered using the :meth:`~Enrollment.Enrollment.having` function. Additionally,
        it is important to note that prior to including this function the analyst should create a list of the diagnoses codes they wish to filter by.

        Note:
            The mark function should only be utilized once the analyst has filtered their Enrollment object with :meth:`~Enrollment.Enrollment.having`.

        Args:
            condition: :class:`Diagnoses` object: Use the :meth:`~Diagnoses.Diagnoses.where` function to specify a :class:`ServiceCategory`
            marker: `str`: The lable to be populated in the condition column.

        Returns:
            DataFrame: Returns the updated object filtered by the specified chronic condition with a condition column

        Example:
            Create a list of diagnoses codes:

            >>> AFib = ['I230', 'I231', 'I232', 'I233', 'I234', 'I235', 'I236', 'I237', 'I238', 'I213', 'I214', 'I219', 'I220',
                        'I221', 'I222', 'I228', 'I229', 'I21A1', 'I21A9', 'I2101', 'I2102', 'I2109', 'I2111', 'I2119', 'I2121', 'I2129']

            Create an Enrollment object & use the :meth:`~Enrollment.Enrollment.having` function with :meth:`~Diagnoses.Diagnoses.where`
            as a parameter to filter by chronic condition:

            >>> api = Enrollment().byMonth().having(Diagnoses.where(ServiceCategory.inpatient, AFib))

            Return DataFrame:

            >>> display(api.fetch())

            Use the mark function to add a column specifying the chronic condition which the user is filtering by:

            >>> api = Enrollment().byMonth().mark(Diagnoses.where(ServiceCategory.inpatient, AFib), 'AFib')

            Return the more readable version of the DataFrame:

            >>> display(api.fetch())

        """
        from collections import defaultdict

        self.markers[marker] = condition

        self.outer_joins.append(condition.join_outer().format_map(defaultdict(str, parent=self.alias, augment=Palet.augment)))

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _apply_markers(self):
        markers = ''
        for i in self.markers.values():
            markers += ' left join ' + str(i)

        return markers

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _select_markers(self):
        indicators = []
        for key, _val in self.markers.items():

            indicators.append(f"{_val.alias}.indicator as {key},")

        return '\n\t\t\t'.join(indicators)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _select_indicators(self):
        indicators = []
        for key, _val in self.markers.items():

            indicators.append(f"coalesce({_val.alias}.indicator, 0) as {key},")

        if len(indicators) > 0:
            return '\n\t\t\t'.join(indicators)
        else:
            return ''

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _do_calculations(self):
        calculations = []
        for cb in self.calculations:
            calculations.append(cb().format(parent=self.alias))

        if len(calculations) > 0:
            return '\n\t\t\t'.join(calculations)
        else:
            return ''

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _groupby_indicators(self):
        groupby = []
        for key, _val in self.markers.items():
            groupby.append(f"{key},")

        return '\n\t\t\t'.join(groupby)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _groupby_markers(self):
        groupby = []
        for _key, _val in self.markers.items():
            groupby.append(f",{_val.alias}.indicator")

        return '\n\t\t'.join(groupby)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def having(self, constraint):
        """
        The having function, allows user to filter Enrollment objects by chronic conidition diagnoses.
        The :meth:`~Diagnoses.Diagnoses.where` and :meth:`~Diagnoses.Diagnoses.within` from :class:`Diagnoses`.
        Additionally, it is important to note that prior to including this function the analyst should create a list of the diagnoses codes
        they wish to filter by.

        Args:
            constraint: :class:`Diagnoses` object: Use the :meth:`~Diagnoses.Diagnoses.where` function to specify a :class:`ServiceCategory`

        Returns:
            DataFrame: Returns the updated object filtered by the specified chronic condition.

        Example:
            Create a list of diagnoses codes:

            >>> AFib = ['I230', 'I231', 'I232', 'I233', 'I234', 'I235', 'I236', 'I237', 'I238', 'I213', 'I214', 'I219', 'I220',
                        'I221', 'I222', 'I228', 'I229', 'I21A1', 'I21A9', 'I2101', 'I2102', 'I2109', 'I2111', 'I2119', 'I2121', 'I2129']

            Create an Enrollment object & use the having function with :meth:`~Diagnoses.Diagnoses.where` as a parameter to filter by chronic condition:

            >>> api = Enrollment.ByMonth().having(Diagnoses.where(ServiceCategory.inpatient, AFib))

            Return DataFrame:

            >>> display(api.fetch())

            Use the :meth:`~Enrollment.Enrollment.mark` function to add a column specifying the chronic condition which the user is filtering by:

            >>> api = Enrollment([6280]).byMonth().mark(Diagnoses.where(ServiceCategory.inpatient, AFib), 'AFib')

            Return the more readable version of the DataFrame:

            >>> display(api.fetch())

        """
        from collections import defaultdict

        if constraint not in self.having_constraints:
            constraint.filter = self.filter
            self.having_constraints.append(constraint.join_inner().format_map(defaultdict(str, parent=self.nested_alias)))

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _apply_constraints(self):
        contraints = ''
        for i in self.having_constraints:
            contraints += '\ninner join ' + str(i)

        return contraints

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def calculate(self, paletable):
        """
        The calculate function is utilized when combing :class:`Paletable` objects with sub-objects like :class:`ClaimsAnalysis` objects. Where
        :meth:`~Enrollment.Enrollment.having` is used to constrain a query, calculate does not constrain queries, but instead computes columns that
        provide additional context. When :class:`Readmits` is called using this function columns for admits, readmits, and readmit rate are included
        in addition to counts for enrollment. Similarly, when calculate is combined with :class:`Cost` the dataframe with include additional columns
        that contain metrics on the cost of services.

        Args:
            paletable:` :class:`ClaimsAnalysis` object `: Enter a claims analysis object like :class:`Readmits` or :class:`Cost`.

        Example:
            Create a :class:`Cost` object:

            >>> cost = Cost.inpatient()

            Create an Enrollment object with a :class:`cost` object:

            >>> api = Enrollment().calculate(cost)

            Convert to a DataFrame and return:

            >>> df = api.fetch()

            >>> display(df)

            Alternatively, create an Enrollment object with a :class:`Readmits` object:

            >>> api = Enrollment().calculate(Readmits.allcause(30))

            Convert to a DataFrame and return:

            >>> df = api.fetch()

            >>> display(df)

        """
        from collections import defaultdict

        if paletable not in self.calculations:
            paletable.filter = self.filter

            self.calculations.append(paletable.callback)

            self.outer_joins.append(paletable.join_outer().format_map(defaultdict(str, parent=self.alias, augment=Palet.augment)))

        return self

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def _joinsOnYearMon(self):

        outer_joins = ''
        for j in self.outer_joins:
            outer_joins += '\nleft join ' + str(j)

        return outer_joins

    # ---------------------------------------------------------------------------------
    #
    #
    #  SQL Alchemy for Enrollment series by year or year/month for Medicaid and CHIP
    #
    #
    # ---------------------------------------------------------------------------------
    def sql(self, isFetch: bool = False):
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
        if self._sql is None or isFetch:
            super().sql()

            z = f"""
                select
                    counter,
                    { self._getByGroupWithAlias(self.alias) }
                    { self._getDerivedTypeSelections() }
                    { self._getAggregateGroup() }
                    { self._selectTimeunit(self.alias) }
                    { self._select_indicators() }
                    { self._do_calculations() }
                    { self._userDefinedSelect('case') }
                    sum(mdcd_enrollment) as mdcd_enrollment,
                    sum(chip_enrollment) as chip_enrollment
                from (
                    select
                        { self._getByGroupWithAlias() }
                        aa.de_fil_dt,
                        { Palet.joinable('aa.submtg_state_cd') },
                        aa.msis_ident_num,
                        { self._userDefinedSelect('inner') }
                        { self._getTimeUnitBreakdown() }
                        { PaletMetadata.Enrichment._renderAgeRange(self) }
                    from
                        taf.taf_ann_de_base as aa
                        { self._apply_constraints() }
                    where
                        aa.da_run_id in ( {self.date_dimension.relevant_runids('BSE') } ) and
                        { self._sqlFilterWhereClause(self.nested_alias, sqlloc="inner") }
                    group by
                        { self._getByGroupWithAlias() }
                        { self._getDerivedByTypeGroup() }
                        { self._userDefinedSelect('inner') }
                        aa.de_fil_dt,
                        { Palet.joinable('aa.submtg_state_cd', True) },
                        aa.msis_ident_num
                    order by
                        { self._getByGroupWithAlias() }
                        { self._getDerivedByTypeGroup() }
                        { self._userDefinedSelect('inner') }
                        aa.de_fil_dt,
                        { Palet.joinable('aa.submtg_state_cd', True) },
                        aa.msis_ident_num
                ) as {self.alias}

                { self._joinsOnYearMon() }

                where
                    { self._getOuterSQLFilter(PaletMetadata.Enrollment.sqlstmts.outer_filter) } and
                    { self._sqlFilterWhereClause(self.alias) } and
                    { self._derivedTypesWhereClause() }

                group by
                    counter,
                    { self._getByGroupWithAlias(self.alias) }
                    { self._groupby_indicators() }
                    { self._getDerivedTypeSelections() }
                    { self._userDefinedSelect('outer') }
                    { self._getAggregateGroup() }
                    { self._groupTimeunit(self.alias) }
                order by
                    { self._getByGroupWithAlias(self.alias) }
                    { self._groupby_indicators() }
                    { self._getDerivedTypeSelections() }
                    { self._userDefinedSelect('outer') }
                    { self._getAggregateGroup() }
                    { self._groupTimeunit(self.alias) }
            """

            # self._addPostProcess(self._percentChange)
            self._sql = z
        else:
            return self._sql
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
