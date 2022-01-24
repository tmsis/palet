
# TODO: Find out if analysts want acronyms spelled out or are ok with something like Third Party Liability instead of TPL
class AnnualDE():

    columns = {
        'Link Key': 'de_link_key',
        'File Date': 'de_fil_dt',
        'Annual DE Version': 'ann_de_vrsn',
        'Medicaid ID Number': 'msis_ident_num',
        'Social Security Number': 'ssn_num',
        'Birth Date': 'birth_dt',
        'Death Date': 'death_dt',
        'Beneficiary Died During the Year': 'dcsd_flag',
        'Age': 'age_num',
        'Age Group Flag': 'age_grp_flag',
        'Gender': 'gndr_cd',
        'Marital Status': 'mrtl_stus_cd',
        'Income': 'incm_cd',
        'Is a Veteran': 'vet_ind',
        'Is a Citizen': 'ctznshp_ind',
        'Is a Verified Citizen': 'ctznshp_vrfctn_ind',
        'Is an Immigrant': 'imgrtn_stus_cd',
        'Is a Verified Immigrant': 'imgrtn_vrfctn_ind',
        'Immigration Status Five Year Bar End Date': 'imgrtn_stus_5_yr_bar_end_dt',
        'Primary Language': 'othr_lang_home_cd',
        'Constructed Primary Language Group': 'prmry_lang_flag',
        'English Language Proficiency': 'prmry_lang_englsh_prfcncy_cd',
        'Household Size': 'hsehld_size_cd',
        'January Pregnancy': 'prgncy_flag_01',
        'February Pregnancy': 'prgncy_flag_02',
        'March Pregnancy': 'prgncy_flag_03',
        'April Pregnancy': 'prgncy_flag_04',
        'May Pregnancy': 'prgncy_flag_05',
        'June Pregnancy': 'prgncy_flag_06',
        'July Pregnancy': 'prgncy_flag_07',
        'August Pregnancy': 'prgncy_flag_08',
        'September Pregnancy': 'prgncy_flag_09',
        'October Pregnancy': 'prgncy_flag_10',
        'November Pregnancy': 'prgncy_flag_11',
        'December Pregnancy': 'prgncy_flag_12',
        'Was Pregnancy This Year': 'prgncy_flag_evr',
        'Certified American Indian/Alaskan Native': 'crtfd_amrcn_indn_alskn_ntv_ind',
        'Ethnicity': 'ethncty_cd',
        'Race and Ethnicity': 'race_ethncty_flag',
        'Expanded Race and Ethnicity': 'race_ethncty_exp_flag',
        'ZIP Code for Beneficiary Home or Mailing Address': 'elgbl_zip_cd',
        'County for Beneficiary Home or Mailing Address': 'elgbl_cnty_cd',
        'State for Beneficiary Home or Mailing Address': 'elgbl_state_cd',
        'January Eligibility': 'elgblty_grp_cd_01',
        'February Eligibility': 'elgblty_grp_cd_02',
        'March Eligibility': 'elgblty_grp_cd_03',
        'April Eligibility': 'elgblty_grp_cd_04',
        'May Eligibility': 'elgblty_grp_cd_05',
        'June Eligibility': 'elgblty_grp_cd_06',
        'July Eligibility': 'elgblty_grp_cd_07',
        'August Eligibility': 'elgblty_grp_cd_08',
        'September Eligibility': 'elgblty_grp_cd_09',
        'October Eligibility': 'elgblty_grp_cd_10',
        'November Eligibility': 'elgblty_grp_cd_11',
        'December Eligibility': 'elgblty_grp_cd_12',
        'Latest Eligibility Code': 'elgblty_grp_cd_ltst',
        'January Maintenance Assistance Status and Basis of Eligibility': 'masboe_cd_01',
        'Febuary Maintenance Assistance Status and Basis of Eligibility': 'masboe_cd_02',
        'March Maintenance Assistance Status and Basis of Eligibility': 'masboe_cd_03',
        'April Maintenance Assistance Status and Basis of Eligibility': 'masboe_cd_04',
        'May Maintenance Assistance Status and Basis of Eligibility': 'masboe_cd_05',
        'June Maintenance Assistance Status and Basis of Eligibility': 'masboe_cd_06',
        'July Maintenance Assistance Status and Basis of Eligibility': 'masboe_cd_07',
        'August Maintenance Assistance Status and Basis of Eligibility': 'masboe_cd_08',
        'September Maintenance Assistance Status and Basis of Eligibility': 'masboe_cd_09',
        'October Maintenance Assistance Status and Basis of Eligibility': 'masboe_cd_10',
        'November Maintenance Assistance Status and Basis of Eligibility': 'masboe_cd_11',
        'December Maintenance Assistance Status and Basis of Eligibility': 'masboe_cd_12',
        'Latest Maintenance Assistance Status and Basis of Eligibility': 'masboe_cd_ltst',
        'Care Level Status': 'care_lvl_stus_cd',
        'Was Deaf During Year': 'deaf_dsbl_flag_evr',
        'Was Blind During Year': 'blnd_dsbl_flag_evr',
        'Difficulty Concentrating - Ever in Year': 'dfclty_cncntrtng_dsbl_flag_evr',
        'Difficulty Walking - Ever in Year': 'dfclty_wlkg_dsbl_flag_evr',
        'Difficulty Dressing or Bathing - Ever in Year': 'dfclty_drsng_bth_dsbl_flag_evr',
        'Difficulty Running Errands Alone - Ever in Year': 'dfclty_ernds_aln_dsbl_flag_evr',
        'Other Disability - Ever in Year': 'othr_dsbl_flag_evr',
        'Encrypted TMSIS Case Number': 'msis_case_num',
        'January Medicaid Enrollment Days': 'mdcd_enrlmt_days_01',
        'February Medicaid Enrollment Days': 'mdcd_enrlmt_days_02',
        'March Medicaid Enrollment Days': 'mdcd_enrlmt_days_03',
        'April Medicaid Enrollment Days': 'mdcd_enrlmt_days_04',
        'May Medicaid Enrollment Days': 'mdcd_enrlmt_days_05',
        'June Medicaid Enrollment Days': 'mdcd_enrlmt_days_06',
        'July Medicaid Enrollment Days': 'mdcd_enrlmt_days_07',
        'August Medicaid Enrollment Days': 'mdcd_enrlmt_days_08',
        'September Medicaid Enrollment Days': 'mdcd_enrlmt_days_09',
        'October Medicaid Enrollment Days': 'mdcd_enrlmt_days_10',
        'November Medicaid Enrollment Days': 'mdcd_enrlmt_days_11',
        'December Medicaid Enrollment Days': 'mdcd_enrlmt_days_12',
        'Latest Medicaid Enrollment Days': 'mdcd_enrlmt_days_yr',
        'January CHIP Enrollment Days': 'chip_enrlmt_days_01',
        'February CHIP Enrollment Days': 'chip_enrlmt_days_02',
        'March CHIP Enrollment Days': 'chip_enrlmt_days_03',
        'April CHIP Enrollment Days': 'chip_enrlmt_days_04',
        'May CHIP Enrollment Days': 'chip_enrlmt_days_05',
        'June CHIP Enrollment Days': 'chip_enrlmt_days_06',
        'July CHIP Enrollment Days': 'chip_enrlmt_days_07',
        'August CHIP Enrollment Days': 'chip_enrlmt_days_08',
        'September CHIP Enrollment Days': 'chip_enrlmt_days_09',
        'October CHIP Enrollment Days': 'chip_enrlmt_days_10',
        'November CHIP Enrollment Days': 'chip_enrlmt_days_11',
        'December CHIP Enrollment Days': 'chip_enrlmt_days_12',
        'Total CHIP Enrollment Days': 'chip_enrlmt_days_yr',
        'January CHIP Code': 'chip_cd_01',
        'February CHIP Code': 'chip_cd_02',
        'March CHIP Code': 'chip_cd_03',
        'April CHIP Code': 'chip_cd_04',
        'May CHIP Code': 'chip_cd_05',
        'June CHIP Code': 'chip_cd_06',
        'July CHIP Code': 'chip_cd_07',
        'August CHIP Code': 'chip_cd_08',
        'September CHIP Code': 'chip_cd_09',
        'October CHIP Code': 'chip_cd_10',
        'November CHIP Code': 'chip_cd_11',
        'December CHIP Code': 'chip_cd_12',
        'Latest CHIP Code': 'chip_cd_ltst',
        'Beneficiary MBI': 'mdcr_bene_id',
        'Beneficiary Health Insurance Claim (HIC) Number': 'mdcr_hicn_num',
        'January State-Specific Eligibility Group Code': 'state_spec_elgblty_grp_01',
        'February State-Specific Eligibility Group Code': 'state_spec_elgblty_grp_02',
        'March State-Specific Eligibility Group Code': 'state_spec_elgblty_grp_03',
        'April State-Specific Eligibility Group Code': 'state_spec_elgblty_grp_04',
        'May State-Specific Eligibility Group Code': 'state_spec_elgblty_grp_05',
        'June State-Specific Eligibility Group Code': 'state_spec_elgblty_grp_06',
        'July State-Specific Eligibility Group Code': 'state_spec_elgblty_grp_07',
        'August State-Specific Eligibility Group Code': 'state_spec_elgblty_grp_08',
        'September State-Specific Eligibility Group Code': 'state_spec_elgblty_grp_09',
        'October State-Specific Eligibility Group Code': 'state_spec_elgblty_grp_10',
        'November State-Specific Eligibility Group Code': 'state_spec_elgblty_grp_11',
        'December State-Specific Eligibility Group Code': 'state_spec_elgblty_grp_12',
        'Latest State-Specific Eligibility Group Code': 'state_spec_elgblty_grp_ltst',
        'January Medicare-Medicaid Dual Eligibility Code': 'dual_elgbl_cd_01',
        'February Medicare-Medicaid Dual Eligibility Code': 'dual_elgbl_cd_02',
        'March Medicare-Medicaid Dual Eligibility Code': 'dual_elgbl_cd_03',
        'April Medicare-Medicaid Dual Eligibility Code': 'dual_elgbl_cd_04',
        'May Medicare-Medicaid Dual Eligibility Code': 'dual_elgbl_cd_05',
        'June Medicare-Medicaid Dual Eligibility Code': 'dual_elgbl_cd_06',
        'July Medicare-Medicaid Dual Eligibility Code': 'dual_elgbl_cd_07',
        'August Medicare-Medicaid Dual Eligibility Code': 'dual_elgbl_cd_08',
        'September Medicare-Medicaid Dual Eligibility Code': 'dual_elgbl_cd_09',
        'October Medicare-Medicaid Dual Eligibility Code': 'dual_elgbl_cd_10',
        'November Medicare-Medicaid Dual Eligibility Code': 'dual_elgbl_cd_11',
        'December Medicare-Medicaid Dual Eligibility Code': 'dual_elgbl_cd_12',
        'Latest Medicare-Medicaid Dual Eligibility Code': 'dual_elgbl_cd_ltst',
        'January Managed Care Plan Type Code': 'mc_plan_type_cd_01',
        'February Managed Care Plan Type Code': 'mc_plan_type_cd_02',
        'March Managed Care Plan Type Code': 'mc_plan_type_cd_03',
        'April Managed Care Plan Type Code': 'mc_plan_type_cd_04',
        'May Managed Care Plan Type Code': 'mc_plan_type_cd_05',
        'June Managed Care Plan Type Code': 'mc_plan_type_cd_06',
        'July Managed Care Plan Type Code': 'mc_plan_type_cd_07',
        'August Managed Care Plan Type Code': 'mc_plan_type_cd_08',
        'September Managed Care Plan Type Code': 'mc_plan_type_cd_09',
        'October Managed Care Plan Type Code': 'mc_plan_type_cd_10',
        'November Managed Care Plan Type Code': 'mc_plan_type_cd_11',
        'December Managed Care Plan Type Code': 'mc_plan_type_cd_12',
        'January Scope of Medicaid or CHIP Benefits': 'rstrctd_bnfts_cd_01',
        'February Scope of Medicaid or CHIP Benefits': 'rstrctd_bnfts_cd_02',
        'March Scope of Medicaid or CHIP Benefits': 'rstrctd_bnfts_cd_03',
        'April Scope of Medicaid or CHIP Benefits': 'rstrctd_bnfts_cd_04',
        'May Scope of Medicaid or CHIP Benefits': 'rstrctd_bnfts_cd_05',
        'June Scope of Medicaid or CHIP Benefits': 'rstrctd_bnfts_cd_06',
        'July Scope of Medicaid or CHIP Benefits': 'rstrctd_bnfts_cd_07',
        'August Scope of Medicaid or CHIP Benefits': 'rstrctd_bnfts_cd_08',
        'September Scope of Medicaid or CHIP Benefits': 'rstrctd_bnfts_cd_09',
        'October Scope of Medicaid or CHIP Benefits': 'rstrctd_bnfts_cd_10',
        'November Scope of Medicaid or CHIP Benefits': 'rstrctd_bnfts_cd_11',
        'December Scope of Medicaid or CHIP Benefits': 'rstrctd_bnfts_cd_12',
        'Latest Scope of Medicaid or CHIP Benefits': 'rstrctd_bnfts_cd_ltst',
        'Social Security Disability Insurance - Latest in Year': 'ssdi_ind',
        'Supplemental Security Income - Latest in Year': 'ssi_ind',
        'Supplemental Security Income State Supplement - Latest in Year': 'ssi_state_splmt_stus_cd',
        'Supplemental Security Income Status': 'ssi_stus_cd',
        'Birth to Conception - Latest in Year': 'birth_cncptn_ind',
        'Temporary Assistance for Needy Families Cash - Latest in Year': 'tanf_cash_cd',
        'Third Party Liability Insurance Coverage - Latest in Year': 'tpl_insrnc_cvrg_ind',
        'Third Party Liability Other Coverage - Latest in Year': 'tpl_othr_cvrg_ind',
        'Beneficiary Record in Supplemental Dates File': 'el_dts_splmtl',
        'Supplemental Managed Care File': 'mngd_care_splmtl',
        'Beneficiary HCBS Record in Supplemental Disability File': 'hcbs_cond_splmtl',
        'Beneficiary Lock-In Record in Supplemental Disability File': 'lckin_splmtl',
        'Beneficiary Long-Term Services & Supports Record in Supplemental Disability File': 'ltss_splmtl',
        'Beneficiary Record in Supplemental Money Follows Person File': 'mfp_splmtl',
        'Beneficiary Record in Supplemental Health Home and State Plan Option File': 'hh_spo_splmtl',
        'Beneficiary Other Needs Record in Supplemental Disability File': 'other_needs_splmtl',
        'Beneficiary Record in Supplemental Waiver File': 'waiver_splmtl',
        'Record Added Timestamp': 'rec_add_ts',
        'Record Updated Timestamp': 'rec_updt_ts',
        'January Missing Enrollment Type Code': 'misg_enrlmt_type_ind_01',
        'February Missing Enrollment Type Code': 'misg_enrlmt_type_ind_02',
        'March Missing Enrollment Type Code': 'misg_enrlmt_type_ind_03',
        'April Missing Enrollment Type Code': 'misg_enrlmt_type_ind_04',
        'May Missing Enrollment Type Code': 'misg_enrlmt_type_ind_05',
        'June Missing Enrollment Type Code': 'misg_enrlmt_type_ind_06',
        'July Missing Enrollment Type Code': 'misg_enrlmt_type_ind_07',
        'August Missing Enrollment Type Code': 'misg_enrlmt_type_ind_08',
        'September Missing Enrollment Type Code': 'misg_enrlmt_type_ind_09',
        'October Missing Enrollment Type Code': 'misg_enrlmt_type_ind_10',
        'November Missing Enrollment Type Code': 'misg_enrlmt_type_ind_11',
        'December Missing Enrollment Type Code': 'misg_enrlmt_type_ind_12',
        'Missing Eligibility Record for All Months of Year': 'misg_elgblty_data_ind',
        'TAF Production Run Identifier': 'da_run_id',
        'Submitting State': 'submtg_state_cd'
    }


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
