
class Introspection:

    def __init__(self, variables):
        self.variables = variables

        self.memberCols = []
        self.memberColNames = []
        self.memberColsWithAlias = []
        self.memberJoinCols = []
        self.memberColsSkewed = []
        self.memberColsSkewedWithAlias = []

        self.claimCols = []
        self.claimColNames = []
        self.claimColsWithAlias = []
        self.claimJoinCols = []
        self.claimColsWithoutSOC = []

        self.intersectingCols = []
        self.intersectingJoinCols = []

        self.memberJoinColsSkewed = []

    @staticmethod
    def intersection(lst1, lst2):
        return list(set(lst1) & set(lst2))

    @staticmethod
    def skew(lst1, lst2):
        return list(set(lst1) ^ set(lst2))

    def profile(self, result):
        resultCols = []
        obs = result.fetchall()
        for col in obs:
            resultCols.append(col[0])
        return Introspection.intersection(self.variables, resultCols)

    def connect(self):

        # Member Cols
        myResultSet = spark.sql(
            "select column_name from information_schema.columns where table_name = '' ")
        self.memberCols = self.profile(myResultSet)
        self.memberColNames = self.memberCols
        print(self.memberCols)

        # Claim Cols
        myResultSet = spark.sql(
            "select column_name from information_schema.columns where table_name = '' ")
        self.claimCols = self.profile(myResultSet)

        # -------------------------------------
        self.memberColsSkewed = set(self.memberCols) ^ set(self.claimCols)
        self.claimCols = ', '.join(self.claimCols)
        # -------------------------------------
        self.memberColsWithAlias = ['m.' + c for c in self.memberCols]
        self.memberJoinCols = ['m.' + c + ' = a.' + c for c in self.memberCols]
        self.memberColsSkewedWithAlias = [
            'm.' + c for c in self.memberColsSkewed]

        # post-processing
        self.memberCols = ', '.join(self.memberCols)
        self.memberColNames = ', '.join(self.memberColNames)
        self.memberColsWithAlias = ', '.join(self.memberColsWithAlias)
        self.memberJoinCols = ' and '.join(self.memberJoinCols)
        self.memberColsSkewed = ', '.join(self.memberColsSkewed)
        self.memberColsSkewedWithAlias = ', '.join(self.memberColsSkewedWithAlias)
