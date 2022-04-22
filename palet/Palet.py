"""
The flagship class the Palet lirbary is built around. This class provides the frame work for other classes in the library.
The Palet module contains the Palet class, the Utils subclass (short for utilities), and attributes for initialization, loading
metadata, and showing data. The Paletable module inherits from this module, and as such all high level objects that inherit from
the Paletable module inherit from Palet as well.
"""

from datetime import datetime
import logging
import secrets
import sys
import os

from typing import Any


class Palet:
    """The class responsible initialization, logging and utilities.
    This class is critical to utilizing the PALET library. It initializes the API, logs data, and provides access to the Utils subclass.

    Example:
        from palet.Palet import Palet

    Note:
        This class is inherited from the Paletable, which is inherited from all high level objecsts. As such this class does not need to be
        manually imported.
    """
    __instance = None
    PERFORMANCE = 15
    RELEASE = 100

    def __new__(cls):
        from datetime import datetime
        cls.__instance = super().__new__(cls)
        cls.now = datetime.now()
        cls.initialize_logger(cls, cls.now)

        cls.version = '1.7.20220404'

        # static metadata dataframes
        cls.apdxc = cls.load_metadata_file(cls, 'apdxc')
        cls.countystate_lookup = cls.load_metadata_file(cls, 'countystate_lookup')
        cls.fmg = cls.load_metadata_file(cls, 'fmg')
        cls.missVar = cls.load_metadata_file(cls, 'missVar')
        cls.prgncy = cls.load_metadata_file(cls, 'prgncy')
        cls.prgncy['Code'] = cls.prgncy['Code'].str.strip()
        cls.prgncy['Code'] = cls.prgncy['Code'].str.upper()
        cls.prgncy['Type'] = cls.prgncy['Type'].str.strip()
        cls.prgncy['Type'] = cls.prgncy['Type'].str.upper()
        cls.prvtxnmy = cls.load_metadata_file(cls, 'prvtxnmy')
        cls.sauths = cls.load_metadata_file(cls, 'sauths')
        cls.schip = cls.load_metadata_file(cls, 'schip')
        cls.splans = cls.load_metadata_file(cls, 'splans')
        cls.st_fips = cls.load_metadata_file(cls, 'st_fips')
        cls.st_name = cls.load_metadata_file(cls, 'st_name')
        cls.st_usps = cls.load_metadata_file(cls, 'st_usps')
        cls.st2_name = cls.load_metadata_file(cls, 'st2_name')
        cls.stabr = cls.load_metadata_file(cls, 'stabr')
        cls.stc_cd = cls.load_metadata_file(cls, 'stc_cd')
        cls.stc_cd['z_tos'] = cls.stc_cd['TypeOfService'].map('{:03d}'.format)

        # cls.logger = None
        cls.logfile = None

        cls.sql = {}

        cls.actual_time = cls.now.strftime('%d%b%Y:%H:%M:%S').upper()  # ddmmmyy:hh:mm:ss

        # SQL Alias cache
        cls._cache_aliases_ = []
        cls._last_used = None

        # ---------------------------------------------------------------------------------
        #
        # Show current release notes on first load of Paletable
        #    This will log.info the release.readme file
        # ---------------------------------------------------------------------------------
        cls.showReleaseNotes()

    # Palet Singleton
    @staticmethod
    def getInstance():
        """ Static access method. """
        if Palet.__instance is None:
            Palet()
            __newPalet = Palet.getInstance()
            __newPalet.logger.debug("return Palet instance " + str(__newPalet))
        return Palet.__instance

    def __init__(self):
        """ Virtual private constructor. """
        if Palet.__instance is not None:
            raise Exception("This class is a singleton! Use getInstance()")
        else:
            Palet.__instance = self
            Palet.showReleaseNotes()

    @staticmethod
    def showReleaseNotes():
        _logger = logging.getLogger('palet_log')
        _std_path = "/dbfs/FileStore/shared_uploads/akira/lib/palet/release.readme"
        _whl_path = "release.readme"
        readme = ""

        if os.path.exists(_std_path):
            file = open(_std_path)
        elif os.path.exists(_whl_path):
            file = open(_whl_path)
        else:
            return

        for line in file:
            readme += line
        file.close()

        release = readme.splitlines()

        for rl in release:
            _logger.release(rl)

    # --------------------------------------------------------------------
    #
    #
    #
    # --------------------------------------------------------------------
    def cache_run_ids(self, field: str = "da_run_id"):
        from pyspark.sql import SparkSession
        """This method of the Palet class is responsible for pulling the most current valid run ids.

        It uses the pyspark library to run a query on the efts_fil_meta TAF table. The most current valid run ids pulled from this method
        are then used in any query run by a high level object like :class:`Enrollment` or :class:`Eligibility`.

        Args:
            self: None - no input required.

        Returns:
            Spark Datarame: Executes the query and returns a Spark Datarame with fil_4th_node_txt, otpt_name, da_run_id, rptg_prd, and fil_dt.

        Note: Only returns the most current run ids available.

        """

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
                    --   and otpt_name = 'TAF_ANN_DE_BASE'
                    and fil_4th_node_txt = 'BSE'
                group by
                    fil_4th_node_txt,
                    otpt_name,
                    da_run_id,
                    rptg_prd,
                    fil_dt
                order by
                    fil_4th_node_txt,
                    otpt_name,
                    da_run_id,
                    rptg_prd,
                    fil_dt
            """

        spark = SparkSession.getActiveSession()
        pdf = spark.sql(z).toPandas()
        return pdf[field].tolist()

    # --------------------------------------------------------------------
    #
    #
    #
    # --------------------------------------------------------------------
    def initialize_logger(self, now: datetime):
        """Attribute that initializes the logger within the Palet class.
        Prints a datetime so it is clear to the user when the code was executed.
        Logging contains multiple levels: INFO, DEBUG, WARNING, ERROR, CRITICAL, and FATAL.

        Args:
            now: `datetime, optional`: Filter a date and time. Can be set but shouldn't. Defaults to when a log line is written.

        Returns:
            Prints a datetime to show when the code was executed.
        """

        logging.addLevelName(Palet.PERFORMANCE, 'PERFORMANCE')
        logging.addLevelName(Palet.RELEASE, 'RELEASE')

        def performance(self, message, *args, **kws):
            self.log(Palet.PERFORMANCE, message, *args, **kws)

        def release(self, message, *args, **kws):
            self.log(Palet.RELEASE, message, *args, **kws)

        logging.Logger.performance = performance
        logging.Logger.release = release

        self.logger = logging.getLogger('palet_log')
        self.logger.addHandler(logging.StreamHandler(stream=sys.stdout))
        self.logger.setLevel(logging.INFO)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)

        if (self.logger.hasHandlers()):
            self.logger.handlers.clear()

        self.logger.addHandler(ch)

    # --------------------------------------------------------------------
    #
    #   Load in Pickle metadata files sourced from Excel
    #
    # --------------------------------------------------------------------
    def load_metadata_file(self, fn):
        """
        Attribute for loading in Pickle metadata files sourced from Excel.

        Args:
            fn: `str`: The name of the pkl file without the extension

        Returns:
            PDF file with the metadata from the specified pkl file.
        """

        import pandas as pd
        import os
        pdf = None

        this_dir, this_filename = os.path.split(__file__)
        pkl = os.path.join(this_dir + '/cfg/', fn + '.pkl')

        pdf = pd.read_pickle(pkl)

        return pdf

    def setLogLevel(self, level: str = "DEBUG"):
        self.logger = logging.getLogger('palet_log')
        self.logger.setLevel(level)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def show(self, v):
        """
        To be Determined.
        """
        print(self.sql[v])

    def reserveSQLAlias(self):
        _next_alias_ = self._clean_alias(secrets.token_urlsafe(6))
        self.logger.debug("Next alias: " + str(_next_alias_))
        self._cache_aliases_.append(_next_alias_)
        self.logger.debug("Current alias cache: " + str(self._cache_aliases_))
        return _next_alias_

    def getSQLAliasStack(self):
        _shallow_copy = self._cache_aliases_.copy()
        _shallow_copy.reverse()
        return _shallow_copy

    def clearAliasStack(self):
        self._cache_aliases_ = []

    def _clean_alias(self, alias: str):
        sp_chars = ['_', '-']
        for char in sp_chars:
            alias = alias.replace(char, '')

        return alias

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    class Utils():
        """Sub-class within the Palet class containing utilities.
        The Utils subclass features static methods that can be used by CMS analysts.
        """

        # ---------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        @staticmethod
        def compress(string):
            """
            Static method to compress a string

            Args:
                string: `str`: The string a user wishes to compress

            Returns:
                A compressed string
            """

            return ' '.join(string.split())

        # ---------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        @staticmethod
        def show(sql):
            """
            Static method allowing the analyst to return the sql functions being run by the palet library.

            Args:
                sql: `str`: The SQL function an analyst is utilizing when logging.

            Returns:
                Prints the SQL query an analyst is using for logging purposes.
            """
            print(Palet.utils.compress(sql.replace('\n', '')))

        @staticmethod
        def createDateRange(year: str):
            """
            Static method for creating a data range when viewing data.

            Args:
                year: `str`: The year an analyst wants to view data for.

            Returns:
                Data from the all 12 months of the specified year.
            """
            range = year + "01-" + year + "12"
            return range

        # Use this static call on an instance of one of your objects such as Enrollment or Trend
        # Pass the instance into this function and you'll get back all the instance variables you
        # have set on it. Useful for seeing what you have on configured on your object
        @staticmethod
        def propertiesOf(Obj: Any):
            return print(Obj.__dict__)

        @staticmethod
        def numDaysInMonth(month: int, year: int):
            if((month == 2) and ((year % 4 == 0) or ((year % 100 == 0) and (year % 400 == 0)))):
                return 29

            elif(month == 2):
                return 28

            elif(month in (1, 3, 5, 7, 8, 10, 12)):
                return 31

            else:
                return 30

        @staticmethod
        def serialize(alias_list: dict):
            import pickle
            pickle.dump(alias_list, 'alias.pkl', 'wb')

        @staticmethod
        def deserialize(filename: str = 'alias.pkl'):
            import pickle
            aliases: dict = pickle.load(filename, 'rb')
            return aliases


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
