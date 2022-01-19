class PaletMetadata:
    class Enrollment:

        fileDate = 'DE_FIL_DT'

        # ---------------------------------------------------------------------------------
        #
        #   Beneficiary Identity
        #
        #
        # ---------------------------------------------------------------------------------
        class identity:
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
            died = 'dcsd_flag'
            gender = 'gndr_cd'
            maritalStatus = 'mrtl_stus_cd'
            income = 'incm_cd'
            vet = 'vet_ind'

        # ---------------------------------------------------------------------------------
        #
        #   Regional
        #
        #
        # ---------------------------------------------------------------------------------
        class locale:
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

        class CHIP:
            yearly = 'chip_enrlmt_days_yr'
            quarter1 = ['chip_enrlmt_days_01', 'chip_enrlmt_days_02', 'chip_enrlmt_days_03']
            quarter2 = ['chip_enrlmt_days_04', 'chip_enrlmt_days_05', 'chip_enrlmt_days_06']
            quarter3 = ['chip_enrlmt_days_07', 'chip_enrlmt_days_08', 'chip_enrlmt_days_09']
            quarter4 = ['chip_enrlmt_days_10', 'chip_enrlmt_days_11', 'chip_enrlmt_days_12']
            half1 = [quarter1, quarter2]
            half2 = [quarter3, quarter4]

            class monthly:
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

        class Medicaid:
            yearly = 'mdcd_enrlmt_days_yr'
            quarter1 = ['mdcd_enrlmt_days_01', 'mdcd_enrlmt_days_02', 'mdcd_enrlmt_days_03']
            quarter2 = ['mdcd_enrlmt_days_04', 'mdcd_enrlmt_days_05', 'mdcd_enrlmt_days_06']
            quarter3 = ['mdcd_enrlmt_days_07', 'mdcd_enrlmt_days_08', 'mdcd_enrlmt_days_09']
            quarter4 = ['mdcd_enrlmt_days_10', 'mdcd_enrlmt_days_11', 'mdcd_enrlmt_days_12']
            half1 = [quarter1, quarter2]
            half2 = [quarter3, quarter4]

            class monthly:
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

        # ---------------------------------------------------------------------------------
        #
        #   TODO:
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
        eligibiltyGroup = 'elgblty_grp_cd'
        primaryEligibilityGroup = 'prmry_elgblty_grp_ind'
        eligibiltyGroupCategory = 'elgblty_grp_ctgry_flag'
        maintenanceAssistanceStatus = 'mas_cd'
        eligibilityBasis = 'elgblty_mdcd_basis_cd'
        masEligibilityBasis = 'masboe_cd'
        stateEligibility = 'state_spec_elgblty_fctr_txt'
        dualEligibilityCode = 'dual_elgbl_cd'
        dualEligibility = 'dual_elgbl_flag'
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
