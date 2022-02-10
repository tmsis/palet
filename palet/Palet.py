"""
The flagship class the Palet lirbary is built around. This class provides the frame work for other classes in the library.
The Palet module contains the Palet class, the Utils subclass (short for utilities), and attributes for initialization, loading
metadata, and showing data. The Paletable module inherits from this module, and as such all high level objects that inherit from
the Paletable module inherit from Palet as well.
"""

from datetime import datetime
import logging

from typing import Any


class Palet():
    """The class responsible initialization, logging and utilities.
    This class is critical to utilizing the PALET library. It initializes the API, logs data, and provides access to the Utils subclass.

    Example:
        from palet.Palet import Palet

    Note:
        This class is inherited from the Paletable, which is inherited from all high level objecsts. As such this class does not need to be
        manually imported.
    """

    PERFORMANCE = 15

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, report_month: str, start_month: str = None, end_month: str = None, run_id: str = None):
        from datetime import datetime
        self.now = datetime.now()
        self.initialize_logger(self.now)

        self.version = '1.2.20220130'

        self.report_month = datetime.strptime(report_month, '%Y%m')
        # self.start_month = datetime.strptime(start_month, '%Y%m')
        # self.end_month = datetime.strptime(end_month, '%Y%m')

        # static metadata dataframes
        self.apdxc = self.load_metadata_file('apdxc')
        self.countystate_lookup = self.load_metadata_file('countystate_lookup')
        self.fmg = self.load_metadata_file('fmg')
        self.missVar = self.load_metadata_file('missVar')
        self.prgncy = self.load_metadata_file('prgncy')
        self.prgncy['Code'] = self.prgncy['Code'].str.strip()
        self.prgncy['Code'] = self.prgncy['Code'].str.upper()
        self.prgncy['Type'] = self.prgncy['Type'].str.strip()
        self.prgncy['Type'] = self.prgncy['Type'].str.upper()
        self.prvtxnmy = self.load_metadata_file('prvtxnmy')
        self.sauths = self.load_metadata_file('sauths')
        self.schip = self.load_metadata_file('schip')
        self.splans = self.load_metadata_file('splans')
        self.st_fips = self.load_metadata_file('st_fips')
        self.st_name = self.load_metadata_file('st_name')
        self.st_usps = self.load_metadata_file('st_usps')
        self.st2_name = self.load_metadata_file('st2_name')
        self.stabr = self.load_metadata_file('stabr')
        self.stc_cd = self.load_metadata_file('stc_cd')
        self.stc_cd['z_tos'] = self.stc_cd['TypeOfService'].map('{:03d}'.format)

        # self.logger = None
        self.logfile = None

        self.sql = {}

        self.actual_time = self.now.strftime('%d%b%Y:%H:%M:%S').upper()  # ddmmmyy:hh:mm:ss

    # --------------------------------------------------------------------
    #
    #
    #
    # --------------------------------------------------------------------
    def cache_run_ids(self):
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
        return pdf['da_run_id'].tolist()

    # --------------------------------------------------------------------
    #
    #
    #
    # --------------------------------------------------------------------
    def initialize_logger(self, now: datetime):
        """Attribute that initializes the logger within the Palet class.
        Prints a datetime so it is clear to the user when the code was executed.

        Args:
            now: `datetime, optional`: Filter a date and time. Can be set but shouldn't. Defaults to when a log line is written.

        Returns:
            Prints a datetime to show when the code was executed.
        """

        file_date = now.strftime('%Y-%m-%d-%H-%M-%S')

        logging.addLevelName(Palet.PERFORMANCE, 'PERFORMANCE')

        def performance(self, message, *args, **kws):
            self.log(Palet.PERFORMANCE, message, *args, **kws)

        logging.Logger.performance = performance

        p_dir = '/tmp/'
        p_filename = 'custom_log_' + file_date + '.log'
        p_logfile = p_dir + p_filename

        self.logger = logging.getLogger('palet_log')
        self.logger.setLevel(logging.INFO)

        fh = logging.FileHandler(p_logfile, mode='a')
        ch = logging.StreamHandler()
        # ch.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        if (self.logger.hasHandlers()):
            self.logger.handlers.clear()

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

        self.logfile = p_logfile
        self.logfilename = p_filename

        self.logger.debug('DQ Measures log file: ' + p_logfile)

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
