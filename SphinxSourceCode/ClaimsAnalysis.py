"""
The ClaimsAnalysis module contains the ClaimsAnalysis class, which is a parent class with attributes that are inherited by its child classes
such as :class:`Redamits` and :class:`Cost`. As it's namesake implies, all child classes of ClaimsAnalysis pertain to claims. ClaimsAnalysis
objects can be viewed as sub-objects of :class:`Paletable` objects. As such they do not return DataFrames on their own; they simply produce
queries that can be joined with Paletable objects to append columns onto DataFrames or change the context of Paletable objects entirely. 
"""

from DateDimension import DateDimension
# -------------------------------------------------------
#
#
#
# -------------------------------------------------------
class ClaimsAnalysis():
    """
    Being a parent class, like :class:`Paletable`, ClaimsAnalysis is not directly interacted with. Instead, its attributes and methods are inherited
    by its child classes. More information can found on these attributes and methods below:
    
    Attributes:
        callback: Used by ClaimsAnalysis objects to initialize methods that create additional columns for the DataFrame. See :meth:`Readmits.Readmits.calculate` and :meth:`Cost.Cost.caclulate`
        alias: The alias that will be used for the ClaimsAnalysis objects SQL query.
        date_dimension: The :class:`DateDimension` object relevant to the ClaimsAnalysis object.
        filter: The attribute responsible for indicating whether a ClaimsAnalysis object needs to be constrained by state.
        join_sql: The methods used produce the SQL query for a ClaimsAnalysis object.

    Methods:

    """

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def __init__(self):
        self.callback = None
        self.alias = None
        self.date_dimension = DateDimension.getInstance()
        self.filter = {}
        self.join_sql = ''

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def sql(self):
        """The SQL query that the ClaimsAnalysis child class uses to pull dataframes.

        This can be called allowing an analyst to view the SQL query in question.

        Args:
            self: None - no input required.

        Returns:
            str: Returns a text string containing the SQL query run by the ClaimsAnalysis child class.

        Example:
            Create a :class:`Cost` object:

            >>> cost = Cost.inpatient()

            Return the query as text:

            >>> print(cost.calculate().sql())

            Create a :class:`Readmit` object:

            >>> readmits = Readmits.allcause(30)

            Return the query as text:

            >>> print(readmits.sql())

        """

        return self.join_sql

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def __str__(self):
        return self.sql()

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def prepare(self):
        """
        The prepare function is a simple "pass" function. Meaning that when this function is executed, it passed to the prepare function
        specific to the child class.

        Note:
            See :meth:`~Cost.Cost.prepare` or :meth:`~Readmits.Readmits.prepare` to view the specific versions of this method.

        """

        pass

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def calculate(self):
        """
        The calculate function is a simple "pass" function. Meaning that when this function is executed, it passed to the prepare function
        specific to the child class.

        Note:
            See :meth:`~Cost.Cost.calculate` or :meth:`~Readmits.Readmits.calculate` to view the specific versions of this method.

        """

        pass

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def apply_filters(self):
        """
        The apply filters function is not directly interacted with by the user, but is called automatically when a query produced by a ClaimsAnalysis
        object will be joined to a :class:`Paletable` object's query that is constrained by state. This ensures that query produced by the ClaimsAnalysis
        object is also constrained by state.

        """

        where = []

        if len(self.filter) > 0:
            for key in self.filter:
                _in_stmt = []
                _join = ""
                if key not in ['SUBMTG_STATE_CD']:
                    continue

                # get the value(s) in case there are multiple
                values = self.filter[key]
                for val in values:
                    _in_stmt.append(f"'{val}'")

                _join = ",".join(_in_stmt)
                where.append(key + ' in (' + _join + ')')

            if len(where) > 0:
                return f"and {' and '.join(where)}"

        else:
            return ''

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def join_inner(self) -> str:
        """
        The join_inner method is responsible for producing a join conidition that is appended to a :class:`Paletable` object's query ensuring
        that the query produced by a ClaimsAnalysis object is properly integrated. As this method's namesake implies this function produces
        an inner join condition.

        """

        self.prepare()

        sql = f"""
                ({self.join_sql}) as {self.alias}
                on
                        {{parent}}.{{augment}}submtg_state_cd = {self.alias}.submtg_state_cd
                    and {{parent}}.msis_ident_num = {self.alias}.msis_ident_num
                    and {{parent}}.de_fil_dt = {self.alias}.year"""

        return sql

    # -------------------------------------------------------
    #
    #
    #
    # -------------------------------------------------------
    def join_outer(self) -> str:
        """
        The join_inner method is responsible for producing a join conidition that is appended to a :class:`Paletable` object's query ensuring
        that the query produced by a ClaimsAnalysis object is properly integrated. As this method's namesake implies this function produces
        an outer join condition.
        
        """

        self.prepare()

        sql = f"""
                ({self.join_sql}) as {self.alias}
                on
                        {{parent}}.{{augment}}submtg_state_cd = {self.alias}.submtg_state_cd
                    and {{parent}}.msis_ident_num = {self.alias}.msis_ident_num
                    and {{parent}}.de_fil_dt = {self.alias}.year
                    and {{parent}}.month = {self.alias}.month"""

        return sql

