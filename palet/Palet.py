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




        @staticmethod
        def createDateRange(year: str) :
            range = year + "01-" + year + "12" 
            return range