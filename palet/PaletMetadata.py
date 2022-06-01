"""
The PaletMetadata module is composed of the PaletMetadata Class its respective subclasses and the Enrichment. The subclassess correspond
to high level objects like Enrollment. These classes work to make TAF data more easily readable. Column names
within TAF data are assigned more readable values. Additionally, some columns have dictionaries within them that explain what
contructed code values correspond to. The Enrichment class exists to decorate DateFrames and make the information PALET derives more readable.
"""


from palet.ServiceCategory import ServiceCategory


class PaletMetadata:
    """
    In addition to the readability aspect discussed above, the PaletMetadata class is essential to converting
    data within the TAF files to the outputs returned by the API. This most often applies to assigning names to
    structered code or implementing column names that are more concise and easily understandable.

    Note:
        It is a best practice to import this class of the PALET library whenever running a high level
        object in the API.

    Example:
        >>> from palet.PaletMetadata import PaletMetadata

    """
    class Enrollment:
        import pandas as pd
        """
        The Enrollment class is a subclass of PaletMetadata. This class primaryily assigns more readable
        and concise column names to the TAF data pertaining to Medicaid and CHIP enrollment. Additionally,
        some of the subclassess within the Enrollment class contain dictionaries that explain the meaning
        of structed code within columns.
        """

        fileDate = 'DE_FIL_DT'
        type = 'chip_cd_01'
        derived_enrollment_field = 'enrollment_type'
        month = 'month'

        derived_columns = ['coverage_type',
                           'eligibility_type',
                           'enrollment_type']

        chip_cd_mon = ['chip_cd_01',
                       'chip_cd_02',
                       'chip_cd_03',
                       'chip_cd_04',
                       'chip_cd_05',
                       'chip_cd_06',
                       'chip_cd_07',
                       'chip_cd_08',
                       'chip_cd_09',
                       'chip_cd_10',
                       'chip_cd_11',
                       'chip_cd_12']

        chip_cd = {
            'null': 'Unknown',
            '0': 'Not Eligible',
            '1': 'Eligible for Medicaid',
            '2': 'Eligible for Medicaid & Medicaid Expansion CHIP',
            '3': 'Eligible for Seperate Title XXI CHIP',
            '4': 'Eligible for Medicaid & Seperate CHIP'
        }

        common_fields = {
            "gndr_cd": "gndr_cd",
            "age_num": "age_num",
        }

        stack_fields = {
            "rstrctd_bnfts_cd": "rstrctd_bnfts_cd",
            "elgblty_grp_cd": "eligibility_type"
        }

        @staticmethod
        def all_common_fields():
            all_fields = {}
            all_fields.update(PaletMetadata.Enrollment.common_fields)
            all_fields.update(PaletMetadata.Enrollment.stack_fields)

            return all_fields

        def _findFullMonthEnrollments(df: pd.DataFrame):
            palet = PaletMetadata.Enrichment._getPaletObj()
            month = df['month']
            year = df['de_fil_dt']
            if df['month'] != palet.Utils.numDaysInMonth(month, year):
                return

        # ---------------------------------------------------------------------------------
        #
        #   Beneficiary Identity
        #
        #
        # ---------------------------------------------------------------------------------
        class identity:
            """
            The Identity class is a subclass within Enrollment. This subclass assigns column names to
            beneficiary demographic data such as name, date of birth, SSN, etc.
            """

            mediciadId = 'msis_ident_num'
            ssn = 'ssn_num'
            ssnIndicator = 'ssn_ind'
            ssnPending = 'ssn_vrfctn_ind'
            medicareId = 'mdcr_bene_id'
            singleEnrollment = 'sngl_enrlmt_flag'
            firstName = 'elgbl_1st_name'
            lastName = 'elgbl_last_name'
            middleName = 'elgbl_mdl_initl_name'
            dob = 'birth_dt'
            dod = 'death_dt'
            age = 'age_num'
            ageGroup = 'age_grp_flag'
            age_band = 'age_band'
            died = 'dcsd_flag'
            gender = 'gndr_cd'
            maritalStatus = 'mrtl_stus_cd'
            income = 'incm_cd'
            vet = 'vet_ind'

            # class lookup
            incm_cd = {
                '01': '1-100% of FPL',
                '02': '101-133% of FPL',
                '03': '134-150% of FPL',
                '04': '151-200% of FPL',
                '05': '201-255% of FPL',
                '06': '256-300% of FPL',
                '07': '301-400% of FPL',
                '08': '>400% of FPL'
            }

            age_grp_flag = {
                '1': '<1',
                '2': '1-5',
                '3': '6-14',
                '4': '15-18',
                '5': '19-20',
                '6': '21-44',
                '7': '45-64',
                '8': '65-74',
                '9': '75-84',
                '10': '85-125'
            }

        # ---------------------------------------------------------------------------------
        #
        #   Regional
        #
        #
        # ---------------------------------------------------------------------------------
        class locale:
            """
            The locale class is a subclass within Enrollment. This subclass assigns column names to
            beneficiary geographic data such as submitting state, city, mailing address, etc.
            """
            submittingState = 'SUBMTG_STATE_CD'
            address = ['elgbl_line_1_adr_home', 'elgbl_line_2_adr_home', 'elgbl_line_3_adr_home']
            city = 'elgbl_city_name_home'
            zip = 'elgbl_zip_cd_home'
            county = 'elgbl_cnty_cd_home'
            state = 'elgbl_state_cd_home'
            phone = 'elgbl_phne_num_home'
            region = 'reg_flag'
            mailAddress = ['elgbl_line_1_adr_mail', 'elgbl_line_2_adr_mail', 'elgbl_line_3_adr_mail']
            mailCity = 'elgbl_city_name_mail'
            mailZip = 'elgbl_zip_cd_mail'
            mailCounty = 'elgbl_cnty_cd_mail'
            mailState = 'elgbl_state_cd_mail'

        # ---------------------------------------------------------------------------------
        #
        #   Race / Ethnicity
        #
        #
        # ---------------------------------------------------------------------------------
        class raceEthnicity:
            """
            The raceEthnicity class is a subclass within Enrollment. This subclass assigns column names to
            beneficiary race and ethnicity data such as race, ethnicity, Native American certification, etc.
            This subclass also contains dictionaries explaining the values that correspond to the structed code
            in the following columns: race_ethncty_flag, race_ethncty_exp_flag, ethncty_cd, and crtfd_amrcn_indn_alskn_ntv_ind.
            """
            certifiedAmericanIndian = 'crtfd_amrcn_indn_alskn_ntv_ind'
            ethnicity = 'ethncty_cd'
            nativeHawaiian = 'ntv_hi_flag'
            guamanian = 'guam_chamorro_flag'
            samoan = 'samoan_flag'
            otherPacificIslander = 'othr_pac_islndr_flag'
            unknownPacificIslander = 'unk_pac_islndr_flag'
            asian = 'asn_indn_flag'
            chinese = 'chinese_flag'
            filipino = 'filipino_flag'
            japanese = 'japanese_flag'
            korean = 'korean_flag'
            vietmanese = 'vietnamese_flag'
            otherAsian = 'othr_asn_flag'
            unknownAsian = 'unk_asn_flag'
            white = 'wht_flag'
            black = 'black_afrcn_amrcn_flag'
            native = 'aian_flag'
            race = 'race_ethncty_flag'
            raceExpanded = 'race_ethncty_exp_flag'
            hispanic = 'hspnc_ethncty_flag'
            timestampAdd = 'rec_add_ts',
            timestampUpdate = 'rec_updt_ts'

            # class lookup
            race_ethncty_flag = {
                '1': 'white',
                '2': 'black',
                '3': 'asian',
                '4': 'americanIndian',
                '5': 'islander',
                '6': 'mixed',
                '7': 'hispanic'
            }
            race_ethncty_exp_flag = {
                '1': 'white',
                '2': 'black',
                '3': 'americanIndian',
                '4': 'indian',
                '5': 'chinese',
                '6': 'filipino',
                '7': 'japanese',
                '8': 'korean',
                '9': 'vietnamese',
                '10': 'otherAsian',
                '11': 'unknownAsian',
                '12': 'multiAsian',
                '13': 'hawaiian',
                '14': 'guamChamarro',
                '15': 'samoan',
                '16': 'otherIslander',
                '17': 'unkownIslander',
                '18': 'multiIslander',
                '19': 'multiRacial',
                '20': 'hispanic'
            }
            ethncty_cd = {
                '0': 'notHispanic',
                '1': 'mexican',
                '2': 'puertoRican',
                '3': 'cuban',
                '4': 'otherHispanic',
                '5': 'unkownHispanic',
                '6': 'unspecified'
            }
            crtfd_amrcn_indn_alskn_ntv_ind = {
                '0': 'notAmericanIndian',
                '1': 'americanIndian',
                '2': 'certifiedAmericanIndian'
            }

        class CHIP:
            """
            The CHIP class is a subclass within Enrollment. This subclass assigns time periods to
            CHIP enrollment data such as yearly, quarterly, and biannually.
            """
            yearly = 'chip_enrlmt_days_yr'
            quarter1 = ['chip_enrlmt_days_01', 'chip_enrlmt_days_02', 'chip_enrlmt_days_03']
            quarter2 = ['chip_enrlmt_days_04', 'chip_enrlmt_days_05', 'chip_enrlmt_days_06']
            quarter3 = ['chip_enrlmt_days_07', 'chip_enrlmt_days_08', 'chip_enrlmt_days_09']
            quarter4 = ['chip_enrlmt_days_10', 'chip_enrlmt_days_11', 'chip_enrlmt_days_12']
            half1 = [quarter1, quarter2]
            half2 = [quarter3, quarter4]

            class monthly:
                """
                The Monthly class is a subclass within CHIP. This subclass assigns months to
                CHIP enrollment data.
                """
                Jan = 'chip_enrlmt_days_01'
                Feb = 'chip_enrlmt_days_02'
                Mar = 'chip_enrlmt_days_03'
                Apr = 'chip_enrlmt_days_04'
                May = 'chip_enrlmt_days_05'
                Jun = 'chip_enrlmt_days_06'
                Jul = 'chip_enrlmt_days_07'
                Aug = 'chip_enrlmt_days_08'
                Sep = 'chip_enrlmt_days_09'
                Oct = 'chip_enrlmt_days_10'
                Nov = 'chip_enrlmt_days_11'
                Dec = 'chip_enrlmt_days_12'

                enrollment = {
                    '01': "chip_enrlmt_days_01",
                    '02': "chip_enrlmt_days_02",
                    '03': "chip_enrlmt_days_03",
                    '04': "chip_enrlmt_days_04",
                    '05': "chip_enrlmt_days_05",
                    '06': "chip_enrlmt_days_06",
                    '07': "chip_enrlmt_days_07",
                    '08': "chip_enrlmt_days_08",
                    '09': "chip_enrlmt_days_09",
                    '10': "chipenrlmt_days_10",
                    '11': "chip_enrlmt_days_11",
                    '12': "chip_enrlmt_days_12"
                }

        class Medicaid:
            """
            The Medicaid class is a subclass within Enrollment. This subclass assigns time periods to
            CHIP enrollment data such as yearly, quarterly, and biannually.
            """
            yearly = 'mdcd_enrlmt_days_yr'
            quarter1 = ['mdcd_enrlmt_days_01', 'mdcd_enrlmt_days_02', 'mdcd_enrlmt_days_03']
            quarter2 = ['mdcd_enrlmt_days_04', 'mdcd_enrlmt_days_05', 'mdcd_enrlmt_days_06']
            quarter3 = ['mdcd_enrlmt_days_07', 'mdcd_enrlmt_days_08', 'mdcd_enrlmt_days_09']
            quarter4 = ['mdcd_enrlmt_days_10', 'mdcd_enrlmt_days_11', 'mdcd_enrlmt_days_12']
            half1 = [quarter1, quarter2]
            half2 = [quarter3, quarter4]

            class monthly:
                """
                The Monthly class is a subclass within Medicaid. This subclass assigns months to
                Medicaid enrollment data.
                """
                Jan = 'mdcd_enrlmt_days_01'
                Feb = 'mdcd_enrlmt_days_02'
                Mar = 'mdcd_enrlmt_days_03'
                Apr = 'mdcd_enrlmt_days_04'
                May = 'mdcd_enrlmt_days_05'
                Jun = 'mdcd_enrlmt_days_06'
                Jul = 'mdcd_enrlmt_days_07'
                Aug = 'mdcd_enrlmt_days_08'
                Sep = 'mdcd_enrlmt_days_09'
                Oct = 'mdcd_enrlmt_days_10'
                Nov = 'mdcd_enrlmt_days_11'
                Dec = 'mdcd_enrlmt_days_12'

                enrollment = {
                    '01': "mdcd_enrlmt_days_01",
                    '02': "mdcd_enrlmt_days_02",
                    '03': "mdcd_enrlmt_days_03",
                    '04': "mdcd_enrlmt_days_04",
                    '05': "mdcd_enrlmt_days_05",
                    '06': "mdcd_enrlmt_days_06",
                    '07': "mdcd_enrlmt_days_07",
                    '08': "mdcd_enrlmt_days_08",
                    '09': "mdcd_enrlmt_days_09",
                    '10': "mdcd_enrlmt_days_10",
                    '11': "mdcd_enrlmt_days_11",
                    '12': "mdcd_enrlmt_days_12"
                }

        # ---------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        claimNumber = 'mdcr_hicn_num'
        caseNumber = 'msis_case_num'
        citizenship = ['ctznshp_ind', 'ctznshp_vrfctn_ind']
        immigrationStatus = ['imgrtn_stus_cd', 'imgrtn_vrfctn_ind', 'imgrtn_stus_5_yr_bar_end_dt']
        language = ['othr_lang_home_cd', 'prmry_lang_flag', 'prmry_lang_englsh_prfcncy_cd']
        householdSize = 'hsehld_size_cd'
        pregnant = ['prgnt_ind', 'prgncy_flag']
        careLevel = 'care_lvl_stus_cd'
        deaf = 'deaf_dsbl_flag'
        blind = 'blnd_dsbl_flag'
        difficultyConcentrating = 'dfclty_conc_dsbl_flag'
        difficultyWalking = 'dfclty_wlkg_dsbl_flag'
        difficultyBathing = 'dfclty_drsng_bathg_dsbl_flag'
        difficultyErrands = 'dfclty_errands_aln_dsbl_flag'
        disabilityOther = 'othr_dsbl_flag'
        enrollmentType = 'enrl_type_flag'
        eligibleDaysThisMonth = 'days_elig_in_mo_cnt'
        eligibleThisMonth = 'elgbl_entir_mo_ind'
        eligibleLastDayThisMonth = 'elgbl_last_day_of_mo_ind'
        chip = 'chip_cd'
        scope = 'rstrctd_bnfts_cd'
        ssdi = 'ssdi_ind'
        ssi = 'ssi_ind'
        ssiStateCode = 'ssi_state_splmt_stus_cd'
        ssiStatus = 'ssi_stus_cd'
        birthToConception = 'birth_cncptn_ind'
        temporaryAssistance = 'tanf_cash_cd'
        healthMdeicalHome = 'hh_pgm_prtcpnt_flag'
        healthProvider = 'hh_prvdr_num'
        healthEntity = 'hh_ent_name'
        malignantHyperthermia = 'mh_hh_chrnc_cond_flag'
        substanceAbuse = 'sa_hh_chrnc_cond_flag'
        asthma = 'asthma_hh_chrnc_cond_flag'
        diabetes = 'dbts_hh_chrnc_cond_flag'
        heartDisease = 'hrt_dis_hh_chrnc_cond_flag'
        overweight = 'ovrwt_hh_chrnc_cond_flag'
        hiv = 'hiv_aids_hh_chrnc_cond_flag'
        otherChronicConidition = 'othr_hh_chrnc_cond_flag'
        lockInProvider = ['lckin_prvdr_num1', 'lckin_prvdr_num2', 'lckin_prvdr_num3']
        lockInProviderType = ['lckin_prvdr_type_cd1', 'lckin_prvdr_type_cd2', 'lckin_prvdr_type_cd3']
        lockIn = 'lckin_flag'
        longTermProvider = ['ltss_prvdr_num1', 'ltss_prvdr_num2', 'ltss_prvdr_num3']
        longTermCareLevel = ['ltss_lvl_care_cd1', 'ltss_lvl_care_cd2', 'ltss_lvl_care_cd3']
        livesWithFamily = 'mfp_lvs_wth_fmly_cd'
        qualifiedInstituion = 'mfp_qlfyd_instn_cd'
        qualifiedResidence = 'mfp_qlfyd_rsdnc_cd'
        participationEndedReason = 'mfp_prtcptn_endd_rsn_cd'
        reinstitutionalizedReasonCode = 'mfp_rinstlzd_rsn_cd'
        moneyFollowsPerson = 'mfp_prtcpnt_flag'
        cfc = 'cmnty_1st_chs_spo_flag'
        thirdPartyLiabilityInsurance = 'tpl_insrnc_cvrg_ind'
        thirdPartyLiabilityOther = 'tpl_othr_cvrg_ind'
        enrollmentIntervals = 'medicaid_enrollment_intervals'
        CHIPIntervals = 'chip_enrollment_intervals'

    class Eligibility:
        """
        The Eiligibility class is a subclass of PaletMetadata. This class primaryily assigns more readable
        and concise column names to the TAF data pertaining to Medicaid and CHIP eligibility.

        Note:
            Incomplete, work in progress.
        """
        fileDate = 'DE_FIL_DT'
        primaryEligibilityGroup = 'prmry_elgblty_grp_ind'
        eligibiltyGroupCategory = 'elgblty_grp_ctgry_flag'
        maintenanceAssistanceStatus = 'mas_cd'
        eligibilityBasis = 'elgblty_mdcd_basis_cd'
        masEligibilityBasis = 'masboe_cd'
        stateEligibility = 'state_spec_elgblty_fctr_txt'
        dualEligibilityCode = 'dual_elgbl_cd'
        dualEligibility = 'dual_elgbl_flag'
        notEnrolled = 'mdcd_not_enrolled'

        eligibility_cd = {
            "01": "Parents & Other Caretaker Relatives",
            "02": "Transitional Medical Assistance",
            "03": "Extended Medicaid due to Earnings",
            "04": "Extended Medicaid due to Spousal Support Collections",
            "05": "Pregnant Women",
            "06": "Deemed Newborns",
            "07": "Infants & Children under Age 19",
            "08": "Children w/ Title IV-E Adoption Asst, Foster or Guardianship Care",
            "09": "Former Foster Care Children",
            "11": "Indvl Receiving SSI",
            "12": "Aged, Blind & Disabled Indvl in 209(b) States",
            "13": "Indvl Receiving M&atory State Supplements",
            "14": "Indvl Who Are Essential Spouses",
            "15": "Institutionalized Indvl Continuously Eligible Since 1973",
            "16": "Blind or Disabled Indvl Eligible 1973",
            "17": "Indvl Who Lost Eligibility for SSI/SSP Due to Incr. OASDI Benefits in 1972",
            "18": "Indvl Who Would be Eligible SSI/SSP but for OASDI COLA incr. since April, 1977",
            "19": "Disabled Widows & Widowers Ineligible for SSI due to Increase in OASDI",
            "20": "Disabled Widows & Widowers Ineligible for SSI due to Early Rcpt of Social Security",
            "21": "Working Disabled under 1619(b)",
            "22": "Disabled Adult Children",
            "23": "Qualified Medicare Beneficiaries",
            "24": "Qualified Disabled & Working Indvl",
            "25": "Specified Low Income Medicare Beneficiaries",
            "26": "Qualifying Indvl",
            "27": "Optional Coverage of Parents & Other Caretaker Relatives",
            "28": "Reasonable Classifications of Indvl under Age 21",
            "29": "Children w/ Non-IV-E Adoption Assistance",
            "30": "Independent Foster Care Adolescents",
            "31": "Optional Targeted Low Income Children",
            "32": "Indvl Electing COBRA Continuation Coverage",
            "33": "Indvl above 133% FPL under Age 65",
            "34": "Certain Indvl Needing Treatment for Breast or Cervical Cancer",
            "35": "Indvl Eligible for Family Planning Services",
            "36": "Indvl w/ Tuberculosis",
            "37": "Aged, Blind or Dsbld Indiv. Eligible for but Not Receiving Cash Asst",
            "38": "Indvl Eligible for Cash Assistance except for Institutionalization",
            "39": "Indvl Receiving Home & Community Based Services under Institutional Rules",
            "40": "Optional State Suppl Recipients - 1634 States, & SSI Criteria States w/ 1616 Agremts",
            "41": "Optional State Suppl Recipients - 209(b) States, & SSI Criteria States w/out 1616 Agreements",
            "42": "Institutionalized Indvl Eligible under a Special Income Level",
            "43": "Indvl participating in a PACE Program under Institutional Rules",
            "44": "Indvl Receiving Hospice Care",
            "45": "Qualified Disabled Children under Age 19",
            "46": "Poverty Level Aged or Disabled",
            "47": "Work Incentives Eligibility Group",
            "48": "Ticket to Work Basic Group",
            "49": "Ticket to Work Medical Improvements Group",
            "50": "Family Opportunity Act Children w/ Disabilities",
            "51": "Indvl Eligible for Home & Community-Based Services",
            "52": "Indvl Eligible for Home & Community-Based Services - Special Income Level",
            "53": "Medically Needy Pregnant Women",
            "54": "Medically Needy Children under Age 18",
            "55": "Medically Needy Children Age 18 Through 20",
            "56": "Medically Needy Parents & Other Caretakers",
            "59": "Medically Needy Aged, Blind or Disabled",
            "60": "Medically Needy Blind or Disabled Indvl Eligible 1973",
            "61": "Targeted Low-Income Children",
            "62": "Deemed Newborn",
            "63": "Children Ineligible for Medicaid Due to Loss of Income Disregards",
            "64": "Coverage from Conception to Birth",
            "65": "Children w/ Access to Public Employee Coverage",
            "66": "Children Eligible for Dental Only Supplemental Coverage",
            "67": "Targeted Low-Income Pregnant Women",
            "68": "Pregnant Women w/ Access to Public Employee Coverage",
            "69": "Indvl w/ Mental Health Conditions (expansion group)",
            "70": "Family Planning Participants (expansion group)",
            "71": "Other expansion group",
            "72": "Adult Group - Indvl <= 133% FPL Age 19 - 64, newly elgbl for all states",
            "73": "Adult Group - Indvl <= 133% FPL Age 19 - 64, not newly elgbl for non 1905z(3) states",
            "74": "Adult Group - Indvl <= 133% FPL Age 19 - 64, not newly elgbl parent/caretaker-relative(s) in 1905z(3) states",
            "75": "Adult Group - Indvl <= 133% FPL Age 19 - 64, not newly elgbl non-parent/caretaker-relative(s) in 1905z(3) states",
            "76": "Uninsured Individual eligible for COVID-19 testing"

        }

    class Coverage:
        """
        The Coverage class is a subclass of PaletMetadata. This class primaryily assigns more readable
        and concise column names to the TAF data pertaining to Medicaid and CHIP coverage.

        Note:
            Incomplete, work in progress.
        """
        fileDate = 'DE_FIL_DT'
        mc_plan_type_cd = 'coverage_type'
        type = 'coverage_type'
        coverage_type = {
            'null': 'Unknown',
            '01': "Comprehensive Managed Care Organization",
            '02': "Traditional PCCM Provider arrangement",
            '03': "Enhanced PCCM Provider arrangement",
            '04': "Health Insuring Organization (HIO)",
            '05': "Medical-Only PIHP",
            '06': "Medical-Only PAHP",
            '07': "Long-Term Services & Supports (LTSS) PIHP",
            '08': "Mental Health (MH) PIHP",
            '09': "Mental Health (MH) PAHP",
            '10': "(SUD) PIHP",
            '11': "Substance Use Disorders (SUD) PAHP",
            '12': "Mental Health (MH) and (SUD) PIHP",
            '13': "Mental Health (MH) and (SUD) PAHP",
            '14': "Dental PAHP",
            '15': "Transportation PAHP",
            '16': "Disease Management PAHP",
            '17': "Prog All-Inclusive Care for the Elderly (PACE)",
            '18': "Pharmacy PAHP",
            '19': "LT Services & Supports (LTSS) and (MH) PIHP",
            '20': "Other",
            '60': "Accountable Care Organization",
            '70': "Health/Medical Home (retired value)",
            '80': "Integrated Care for Dual Eligibles"
        }

    class Claims:
        ptnt_stus_cd = "ptnt_stus_cd"
        patient_status_values = {
            "01": "Discharged to home/self-care (routine charge).",
            "02": "Discharged/transferred to other short term general hospital for inpatient care. ",
            "03": "Discharged/transferred to skilled nursing facility (SNF) with Medicare certification in anticipation of covered skilled care -- \
                  (For hospitals with an approved swing bed arrangement, use Code 61 swing bed. For reporting discharges/transfers \
                  to a noncertified SNF, the hospital must use Code 04 - ICF. ",
            "04": "Discharged/transferred to intermediate care facility (ICF). ",
            "05": "Discharged/transferred to another type of institution for inpatient care (including distinct parts). ",
            "06": "Discharged/transferred to home care of organized home health service organization. ",
            "07": "Left against medical advice or discontinued care. ",
            "08": "Discharged/transferred to home under care of a home IV drug therapy provider. ",
            "09": "Admitted as an inpatient to this hospital. In situations where a patient is admitted before midnight of the third day \
                  following the day of an outpatient service, the outpatient services are considered inpatient. ",
            "20": "Expired (patient did not recover).",
            "21": "Discharged/transferred to court/law enforcement.",
            "30": "Still patient. ",
            "40": "Expired at home (hospice claims only). ",
            "41": "Expired in a medical facility such as hospital, SNF, ICF, or freestanding hospice. (Hospice claims only). ",
            "42": "Expired - place unknown (Hospice claims only). ",
            "43": "Discharged/transferred to a federal hospital. ",
            "50": "Discharged/transferred to a Hospice - home. ",
            "51": "Discharged/transferred to a Hospice - medical facility. ",
            "61": "Discharged/transferred within this institution to a hospital-based Medicare approved swing bed. ",
            "62": "Discharged/transferred to an inpatient rehabilitation facility including distinct parts units of a hospital.",
            "63": "Discharged/transferred to a long term care hospital. ",
            "64": "Discharged/transferred to a nursing facility certified under Medicaid but not under Medicare. ",
            "65": "Discharged/Transferred to a psychiatric hospital or psychiatric distinct unit of a hospital \
                  (these types of hospitals were pulled from patient/discharge status code '05' and given their own code). ",
            "66": "Discharged/transferred to a Critical Access Hospital (CAH) ",
            "69": "Discharged/transferred to a designated disaster alternative care site (starting 10/2013; applies only to particular MS-DRGs*). ",
            "70": "Discharged/transferred to another type of health care institution not defined elsewhere in code list. ",
            "71": "Discharged/transferred/referred to another institution for outpatient services as specified by the discharge plan of care (eff. 9/01) \
                  (discontinued effective 10/1/05)",
            "72": "Discharged/transferred/referred to this institution for outpatient services as specified by the discharge plan of care (eff. 9/01) \
                  (discontinued effective 10/1/05) The following codes apply only to particular MS-DRGs*, and were new in 10/2013:",
            "81": "Discharged to home or self-care with a planned acute care hospital inpatient readmission. ",
            "82": "Discharged/transferred to a short term general hospital for inpatient care with a planned acute care hospital inpatient readmission. ",
            "83": "Discharged/transferred to a skilled nursing facility (SNF) with Medicare certification with a planned \
                  acute care hospital inpatient readmission. ",
            "84": "Discharged/transferred to a facility that provides custodial or supportive care with a planned acute care hospital inpatient readmission. ",
            "85": "Discharged/transferred to a designated cancer center or children's hospital with a planned acute care hospital inpatient readmission. ",
            "86": "Discharged/transferred to home under care of organized home health service organization with a planned \
                  acute care hospital inpatient readmission. ",
            "87": "Discharged/transferred to court/law enforcement with a planned acute care hospital inpatient readmission. ",
            "88": "Discharged/transferred to a federal health care facility with a planned acute care hospital inpatient readmission. ",
            "89": "Discharged/transferred to a hospital-based Medicare approved swing bed with a planned acute care hospital inpatient readmission. ",
            "90": "Discharged/transferred to an inpatient rehabilitation facility (IRF) including rehabilitation distinct part units of a hospital with a planned \
                  acute care hospital inpatient readmission. ",
            "91": "Discharged/transferred to a Medicare certified long term care hospital (LTCH) with a planned acute care hospital inpatient readmission. ",
            "92": "Discharged/transferred to a nursing facility certified under Medicaid but not certified under Medicare with a planned \
                  acute care hospital inpatient readmission. ",
            "93": "Discharged/transferred to a psychiatric distinct part unit of a hospital with a planned acute care hospital inpatient readmission. ",
            "94": "Discharged/transferred to a critical access hospital (CAH) with a planned acute care hospital inpatient readmission.",
            "95": "Discharged/transferred to another type of health care institution not defined elsewhere in this code list with a planned \
                  acute care hospital inpatient readmission. Null/missing : source value is missing or unknown *MS-DRG codes where \
                  additional codes were available in October 2013 ",
            "280": "(Acute Myocardial Infarction, Discharged Alive with MCC) ",
            "281": "(Acute Myocardial Infarction, Discharged Alive with CC)",
            "282": "(Acute Myocardial Infarction, Discharged Alive without CC/MCC) ",
            "789": "(Neonates, Died or Transferred to Another Acute Care Facility)"
        }

    class Member():
        """
        The Member class is a subclass of PaletMetadata. This class primaryily assigns more readable
        and concise column names to the TAF data pertaining to chronic coniditions. It also plays an important role
        in ensuring the correct file types and run ids are utilized in chronic condition queries.

        Note:
            Incomplete, work in progress.
        """

        service_category = {
            ServiceCategory.inpatient: "data_anltcs_taf_iph_vw",
            ServiceCategory.long_term: "data_anltcs_taf_lth_vw",
            ServiceCategory.other_services: "data_anltcs_taf_oth_vw",
            ServiceCategory.prescription: "data_anltcs_taf_rxh_vw"
        }

        run_id_file = {
            ServiceCategory.inpatient: 'IPH',
            ServiceCategory.other_services: 'OTH',
            ServiceCategory.long_term: 'LTH',
            ServiceCategory.prescription: 'RXH'
        }

    class Enrichment():
        """
        The Enrichment class is responsible for decorating and enhancing DataFrames created by PALET. This class consists of a series of backend
        functions that build out additional columns, include user defined values, and more.

        Note:
            Enrichment is not directly interacted with by analysts and consists solely of back end functions. See the source code for more information.
        """

        import pandas as pd
        from datetime import datetime
        from palet.Palet import Palet

        @staticmethod
        def _getPaletObj():
            enrichment = PaletMetadata.Enrichment()
            palet = enrichment.Palet.getInstance()
            return palet

        @staticmethod
        def _checkForHelperMsg(field, field_type, value_example: str):

            if type(field) != field_type and field is not None:
                PaletMetadata.Enrichment._getPaletObj().logger.error(str(field) + " is not a valid value. Please enter in the form of a " + str(field_type) + " e.g. " + value_example)
                return ",'n/a' as " + str(field)

        def getDefinedColumns(self):
            self.defined_columns = {
                'age_grp_flag': PaletMetadata.Enrichment._buildAgeGroupColumn,
                'race_ethncty_flag': PaletMetadata.Enrichment._buildRaceEthnicityColumn,
                'SUBMTG_STATE_CD': PaletMetadata.Enrichment._mergeStateEnrollments,
                'race_ethncty_exp_flag': PaletMetadata.Enrichment._buildRaceEthnicityExpColumn,
                'ethncty_cd': PaletMetadata.Enrichment._buildEthnicityColumn,
                'enrollment_type': PaletMetadata.Enrichment._buildEnrollmentType,
                'eligibility_type': PaletMetadata.Enrichment._buildEligibilityType,
                'incm_cd': PaletMetadata.Enrichment._buildIncomeColumn,
                'age_band': PaletMetadata.Enrichment._removeAgeBandNotFound,
                'coverage_type': PaletMetadata.Enrichment._buildValueColumn,
                'ptnt_stus_cd': PaletMetadata.Enrichment._buildPatientStatusColumn,
                'isfirst': PaletMetadata.Enrichment._removeIsFirst
            }

            return self.defined_columns

        # --------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _mergeStateEnrollments(df: pd.DataFrame):
            # PaletMetadata.Enrichment.Palet.logger.info('Merging separate state enrollments')
            palet = PaletMetadata.Enrichment._getPaletObj()

            timeunit = 'month'

            if 'year' in df.columns:
                timeunit = 'year'

            df['USPS'] = df['SUBMTG_STATE_CD'].apply(lambda x: str(x).zfill(2))
            df = PaletMetadata.Enrichment.pd.merge(df, palet.st_name,
                                                   how='inner',
                                                   left_on=['USPS'],
                                                   right_on=['USPS'])
            df = PaletMetadata.Enrichment.pd.merge(df, palet.st_usps,
                                                   how='inner',
                                                   left_on=['USPS'],
                                                   right_on=['USPS'])
            df = df.drop(['USPS'], axis=1)

            df.groupby(by=['STABBREV', timeunit]).sum().reset_index()

            return df

        # --------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _findRaceValueName(x):
            # PaletMetadata.Enrichment.Palet.logger.debug('looking up the race_ethncty_flag value from our metadata')
            # import math
            # get this row's ref value from the column by name
            y = x['race_ethncty_flag']
            # lookup label with value
            return PaletMetadata.Enrollment.raceEthnicity.race_ethncty_flag.get(y)

        # ---------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _buildRaceEthnicityColumn(df: pd.DataFrame):
            # PaletMetadata.Enrichment.Palet.logger.debug('build our columns by looking for race value')
            df['race'] = df.apply(lambda x: PaletMetadata.Enrichment._findRaceValueName(x), axis=1)

            return df

        # --------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _findRaceExpValueName(x):
            # self.palet.logger.debug('looking up the race_ethncty_exp_flag value from our metadata')
            # get this row's ref value from the column by name
            y = x['race_ethncty_exp_flag']
            # lookup label with value
            return PaletMetadata.Enrollment.raceEthnicity.race_ethncty_exp_flag.get(y)

        # ---------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _buildRaceEthnicityExpColumn(df: pd.DataFrame):
            # self.palet.logger.debug('build our columns by looking for race_ethncty_exp_flag')
            df['raceExpanded'] = df.apply(lambda x: PaletMetadata.Enrichment._findRaceExpValueName(x), axis=1)

            return df

        # --------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _findEthnicityValueName(x):
            # self.palet.logger.debug('looking up the ethncty_cd value from our metadata')
            # get this row's ref value from the column by name
            y = x[PaletMetadata.Enrollment.raceEthnicity.ethnicity]
            # lookup label with value
            return PaletMetadata.Enrollment.raceEthnicity.ethncty_cd.get(y)

        # ---------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _buildEthnicityColumn(df: pd.DataFrame):
            # self.palet.logger.debug('build our columns by looking for ethncty_cd')
            print("calling build race ethnicity")
            df['ethnicity'] = df.apply(lambda x: PaletMetadata.Enrichment._findEthnicityValueName(x), axis=1)

            return df

        # --------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _findPatientStatusValues(x):
            # self.palet.logger.debug('looking up the ethncty_cd value from our metadata')
            # get this row's ref value from the column by name
            y = x[PaletMetadata.Claims.ptnt_stus_cd]
            # lookup label with value
            return PaletMetadata.Claims.patient_status_values.get(y)

        # ---------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _buildPatientStatusColumn(df: pd.DataFrame):
            # self.palet.logger.debug('build our columns by looking for ethncty_cd')
            print("calling build patient status column")
            df['patient_status'] = df.apply(lambda x: PaletMetadata.Enrichment._findPatientStatusValues(x), axis=1)

            return df

        # --------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _findEnrollmentType(x):
            # self.palet.logger.debug('looking up the enrollmentType value from our metadata')
            # get this row's ref value from the column by name
            y = x[PaletMetadata.Enrollment.derived_enrollment_field]
            # lookup label with value
            return PaletMetadata.Enrollment.chip_cd.get(y)

        def _buildEnrollmentType(df: pd.DataFrame):
            # self.palet.logger.debug('build our columns by looking for enrollmentType')
            df['enrollment_type_label'] = df.apply(lambda x: PaletMetadata.Enrichment._findEnrollmentType(x), axis=1)

            return df

        # --------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _findEligibiltyType(x):
            from palet.EligibilityType import EligibilityType
            # self.palet.logger.debug('looking up the eligibility value from our metadata')
            # get this row's ref value from the column by name
            y = x[EligibilityType.alias]
            # lookup label with value
            return PaletMetadata.Eligibility.eligibility_cd.get(y)

        # --------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _buildEligibilityType(df: pd.DataFrame):
            # self.palet.logger.debug('build our columns by looking for eligibilty codes')
            df['eligibility_category'] = df.apply(lambda x: PaletMetadata.Enrichment._findEligibiltyType(x), axis=1)
            return df

        # --------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _findIncomeValueName(x):
            # self.palet.logger.debug('looking up the incm_cd value from our metadata')
            # get this row's ref value from the column by name
            y = x[PaletMetadata.Enrollment.identity.income]
            # lookup label with value
            return PaletMetadata.Enrollment.identity.incm_cd.get(y)

        # ---------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _buildIncomeColumn(df: pd.DataFrame):
            # self.palet.logger.debug('build our columns by looking for income_cd')
            df['income'] = df.apply(lambda x: PaletMetadata.Enrichment._findIncomeValueName(x), axis=1)

            return df

        # --------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _findAgeGroupValueName(x):
            # get this row's ref value from the column by name
            y = x[PaletMetadata.Enrollment.identity.ageGroup]
            # lookup label with value
            return PaletMetadata.Enrollment.identity.age_grp_flag.get(y)

        # ---------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _buildAgeGroupColumn(df: pd.DataFrame):
            df['ageGroup'] = df.apply(lambda x: PaletMetadata.Enrichment._findAgeGroupValueName(x), axis=1)

            return df

        def _removeAgeBandNotFound(df: pd.DataFrame):
            df = df.loc[df['age_band'] != 'not found']

            return df

        # ---------------------------------------------------------------------------------
        #
        #
        #
        # ---------------------------------------------------------------------------------

        def _renderAgeRange(self):
            if self.age_band is not None:
                PaletMetadata.Enrichment._checkForHelperMsg(self.age_band, dict, "{'Teenager': [13,19],'Twenties': [20,29],'Thirties': [30,39]}")

                ageBandWhere = []

                ageBandWhere.append(', case')

                for i in self.age_band.keys():
                    a = self.age_band[i]
                    ab = f"when age_num >= {a[0]} and age_num <= {a[1]} then '{i}'"
                    ageBandWhere.append(ab)

                ageBandWhere.append("else 'not found' end as age_band")

                return ' '.join(ageBandWhere)

            else:
                return ""

        # ---------------------------------------------------------------------------------
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _findValueName(x):
            y = x['coverage_type']

            return PaletMetadata.Coverage.coverage_type.get(y)

        # ---------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _buildValueColumn(df: pd.DataFrame):

            df['coverage_type_label'] = df.apply(lambda x: PaletMetadata.Enrichment._findValueName(x), axis=1)

            return df

        # --------------------------------------------------------------------------------
        #
        #
        #
        #
        # ---------------------------------------------------------------------------------
        def _findValueName_(x, column, derived_column):
            # get this row's ref value from the column by name
            y = x[column]
            # if the value is NaN, default to unknown
            if y is None or y == 'null':
                return 'unknown'
            else:
                # lookup label with value
                return derived_column.get(y)

        # ---------------------------------------------------------------------------------
        # _buildValueColumn_ is provided to convert codes to values
        # for **kwargs use columnToAdd1, columnToAdd2, etc.
        # ---------------------------------------------------------------------------------
        def _buildValueColumn_(df: pd.DataFrame, **kwargs):
            column1 = kwargs[0]
            df[column1] = df.apply(lambda x: PaletMetadata._findValueName_(x), axis=1)

        # ---------------------------------------------------------------------------------
        #
        #
        #
        # ----------------------------------------------------------------------------------
        def _removeIsFirst(df: pd.DataFrame):
            df = df.drop(columns=['isfirst'], axis=1)
            return df
