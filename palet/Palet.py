class Palet():

    class Utils():

        # ---------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        @staticmethod
        def compress(string):
            return ' '.join(string.split())


        # ---------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        @staticmethod
        def show(sql) :
            print(Palet.utils.compress(sql.replace('\n', '')))


        # ---------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def getValueFromFilter(self, column: str):
            value = self.filter.get(column)
            return column + " = " + value


        # ---------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def checkForMultiVarFilter(values: str, separator=" "):
            return values.split(separator)


        # ---------------------------------------------------------------------------------
        #
        # slice and dice here to create the proper sytax for a where clause
        #
        #
        # ---------------------------------------------------------------------------------
        def defineWhereClause(self):
            clause = ""
            where = []
            for key in self.filter:

                # get the value(s) in case there are multiple
                values = self.filter[key]

                if str(values).find(" ") > -1: #Check for multiple values here, space separator is default
                    splitVals = self.checkForMultiVarFilter(values)
                    for value in splitVals:
                        clause = ("mon." + key, value)
                        where.append(' = '.join(clause))

                elif str(values).find("-") > -1: #Check for multiples with - separator
                    splitVals = self.checkForMultiVarFilter(values, "-")
                    range_stmt = "mon." + key + " between " + splitVals[0] + " and " + splitVals[1]

                    where.append(range_stmt)

                else: #else parse the single value
                    clause = ("mon." + key, self.filter[key])
                    where.append(' = '.join(clause))

            return " AND ".join(where)
