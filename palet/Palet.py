from typing import Any


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


        ## Use this static call on an instance of one of your objects such as Enrollment or Trend
        ## Pass the instance into this function and you'll get back all the instance variables you
        ## have set on it. Useful for seeing what you have on configured on your object
        @staticmethod
        def propertiesOf(Obj: Any): 
            return print(Obj.__dict__)