.. PALET Wiki Test documentation master file, created by
   sphinx-quickstart on Fri Jan  7 15:59:24 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

PALET Wiki [v. 1.6.20220325]
===========================================

Introduction
************
Documentation for the Python Analytics Library Essential Toolkit. Below are links which lead to the documentation for the modules that make up
this library. Additionally the Index link can be used to view every module, class, attribute, and method within the library. The module Index
only displays the high level modules. Finally a search page can be used for ease of navigation. 

Background & Preface
********************
The purpose of PALET is to expediate the process of querying TAF data and rapidly returning easily interpretable data sets. These data sets can 
pertain to a variety of measures useful to CMS analysts, such as Medicaid and CHIP enrollment, eligibility, costs of services, readmissions, 
pending renewals, acute care days and emergency room visits. The modules used to return these data sets can be referred to as Paletable objects. 
See the modules below such as Enrollment, Eligibility, etc. Paletable objects are high level objects that inherit from the Paletable module. 
The Paletable module contains filters that can be used to manipulate the data sets with a variety of filters or by groups. See the documentation 
for each module below for more details.

Setting Up PALET
****************
To properly utilize this library, CMS analysts must follow a specific series of steps. These steps are outlined below:

1. Log into Databricks using the PROD or VAL server
2. PROD is the preferred server for user testing
3. Open a notebook and ensure it is being run on the databricks-palet-uat-prod or databricks-palet-uat-val cluster
      a. Note the cluster is critical to accessing the PALET library
      b. If the cluster isn’t currently running, start the cluster
4. Begin by importing one or more Paletable objects and the PaletMetadata module
5. Example:
      a. from palet.Enrollment import Enrollment
      b. from palet.Eligibility import Eligibility
      c. from palet.PaletMetadata import PaletMetadata

From here, the analyst has more freedom and flexibility to explore and manipulate data as they see fit. Paletable objects can be combined with 
by groups from the Paletable class and overwritten by other Paletable objects. Explicit examples can be found in the modules’ documentation below.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   Coverage
   CoverageType
   Diagnoses
   Eligibility
   Enrollment
   EnrollmentType
   Palet
   Paletable
   PaletMetadata
   ServiceCategory

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`