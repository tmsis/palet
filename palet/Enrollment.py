#from pyspark.sql import SparkSession
class Enrollment():

  by = {}
  by_group = []
  filter = {}
  where = []

  #-----------------------------------------------------------------------
  # Initialize the Enrollment API
  #-----------------------------------------------------------------------
  def __init__(self):
    print('Initializing Enrollment API')

  # ---------------------------------------------------------------------------------
  #
  #
  #
  #
  # ---------------------------------------------------------------------------------
  def __init__(self, start, end):
    self.start = start
    self.end = end
  # ---------------------------------------------------------------------------------
  #
  #
  #
  #
  # ---------------------------------------------------------------------------------
  def medicaid_enrollment_intervals():
    print("Start date: " + self.start + " End date: " + self.end)
  # ---------------------------------------------------------------------------------
  #
  #
  #
  #
  # -------------------------------------------------------------------------------------
  def chip_enrollment_intervals():
    print("Start date: " + self.start + " End date: " + self.end)
  def mc_plans():
    print('mc_plans')
  
  # add any fileDates here
  # TODO: Figure out the best way to accept dates in this API
  def byFileDate(self, fileDate=None):
    if fileDate != None:
      self.filter.update({"BSF_FIL_DT": fileDate})
      self.by_group.append("BSF_FIL_DT")
    return self

  # add any byEthnicity values
  def byEthnicity(self, ethnicity=None):
    if ethnicity != None:
      self.filter.update({"race_ethncty_exp_flag": ethnicity})
      self.by_group.append("race_ethncty_exp_flag")
    return self

  # add any byState values
  def byState(self, state_fips=None):
    if state_fips != None:
      self.filter.update({"SUBMTG_STATE_CD": state_fips})
      self.by_group.append("SUBMTG_STATE_CD")
    return self

  def checkForMultiVarFilter(self, values: str):
    return values.split(" ")

  # slice and dice here to create the proper sytax for a where clause
  def defineWhereClause(self):
    clause = ""
    where = []
    for key in self.filter:
      # get the value(s) in case there are multiple
      values = self.filter[key]
      if str(values).find(" ") > -1: #Check for multiple values here
        splitVals = self.checkForMultiVarFilter(values)
        for value in splitVals:
          clause = (key, value)
          where.append(' = '.join(clause))
      else: #else parse the single value
          clause = (key, self.filter[key])
          where.append(' = '.join(clause))
    return " AND ".join(where)

  def fetch(self):
    #spark = SparkSession.getActiveSession()

    sql = f"""select
          {', '.join(self.by_group)}
        , count(*) as m
        from
          taf.taf_mon_bsf as mon
        inner join
          (
            select distinct
              {', '.join(self.by_group)}
              , max(DA_RUN_ID) as DA_RUN_ID
            from 
              taf.tmp_max_da_run_id
            where
              --BSF_FIL_DT >= 201601 and BSF_FIL_DT <= 201812
              { self.defineWhereClause() }
            group by 
              {', '.join(self.by_group)}
            order by 
              {', '.join(self.by_group)}
          ) as rid 
              on  mon.SUBMTG_STATE_CD = rid.SUBMTG_STATE_CD
              and mon.BSF_FIL_DT = rid.BSF_FIL_DT
              and mon.DA_RUN_ID = rid.DA_RUN_ID
        group by
            {', '.join(self.by_group)}
        order by
            {', '.join(self.by_group)}
    """
   
    return sql

  attributes = {
    
    'mediciadId': 'msis_ident_num',
    'ssn': 'ssn_num',
    'ssnIndicator': 'ssn_ind',
    'ssnPending': 'ssn_vrfctn_ind',
    'medicareId': 'mdcr_bene_id',
    'claimNumber': 'mdcr_hicn_num',
    'region': 'reg_flag',
    'caseNumber': 'msis_case_num',
    'singleEnrollment': 'sngl_enrlmt_flag',
    'enrollmentIntervals': 'medicaid_enrollment_intervals',
    'chipEnrollmentIntervals': 'chip_enrollment_intervals',
    'firstName': 'elgbl_1st_name',
    'lastName': 'elgbl_last_name',
    'middleName': 'elgbl_mdl_initl_name',
    'dob': 'birth_dt',
    'dod': 'death_dt',
    'age': 'age_num',
    'ageGroup': 'age_grp_flag',
    'died': 'dcsd_flag',
    'gender': 'gndr_cd',
    'maritalStatus': 'mrtl_stus_cd',
    'income': 'incm_cd',
    'vet': 'vet_ind',
    'citizenship': ['ctznshp_ind','ctznshp_vrfctn_ind'],
    'immigrationStatus': ['imgrtn_stus_cd','imgrtn_vrfctn_ind', 'imgrtn_stus_5_yr_bar_end_dt'],
    'language': ['othr_lang_home_cd','prmry_lang_flag','prmry_lang_englsh_prfcncy_cd'],
    'householdSize': 'hsehld_size_cd',
    'pregnant': ['prgnt_ind','prgncy_flag',],
    'certifiedAmericanIndian': 'crtfd_amrcn_indn_alskn_ntv_ind',
    'ethnicity': 'ethncty_cd',
    'address' : ['elgbl_line_1_adr_home','elgbl_line_2_adr_home','elgbl_line_3_adr_home'],
    'city': 'elgbl_city_name_home',
    'zip': 'elgbl_zip_cd_home',
    'county': 'elgbl_cnty_cd_home',
    'state': 'elgbl_state_cd_home',
    'phone': 'elgbl_phne_num_home',
    'mailAddress': ['elgbl_line_1_adr_mail','elgbl_line_2_adr_mail','elgbl_line_3_adr_mail'],
    'mailCity': 'elgbl_city_name_mail',
    'mailZip': 'elgbl_zip_cd_mail',
    'mailCounty': 'elgbl_cnty_cd_mail',
    'mailState': 'elgbl_state_cd_mail',
    'careLevel': 'care_lvl_stus_cd',
    'deaf': 'deaf_dsbl_flag',
    'blind': 'blnd_dsbl_flag',
    'difficultyConcentrating': 'dfclty_conc_dsbl_flag',
    'difficultyWalking': 'dfclty_wlkg_dsbl_flag',
    'difficultyBathing': 'dfclty_drsng_bathg_dsbl_flag',
    'difficultyErrands': 'dfclty_errands_aln_dsbl_flag',
    'disabilityOther': 'othr_dsbl_flag',
    'enrollmentType': 'enrl_type_flag',
    'eligibleDaysThisMonth': 'days_elig_in_mo_cnt',
    'eligibleThisMonth': 'elgbl_entir_mo_ind',
    'eligibleLastDayThisMonth': 'elgbl_last_day_of_mo_ind',
    'chip': 'chip_cd',
    'eligibiltyGroup': 'elgblty_grp_cd',
    'primaryEligibilityGroup': 'prmry_elgblty_grp_ind',
    'eligibiltyGroupCategory': 'elgblty_grp_ctgry_flag',
    'maintenanceAssistanceStatus': 'mas_cd',
    'eligibilityBasis': 'elgblty_mdcd_basis_cd',
    'masEligibilityBasis': 'masboe_cd',
    'stateEligibility': 'state_spec_elgblty_fctr_txt',
    'dualEligibilityCode': 'dual_elgbl_cd',
    'dualEligibility': 'dual_elgbl_flag',
    'scope': 'rstrctd_bnfts_cd',
    'ssdi': 'ssdi_ind',
    'ssi': 'ssi_ind',
    'ssiStateCode': 'ssi_state_splmt_stus_cd',
    'ssiStatus': 'ssi_stus_cd',
    'birthToConception': 'birth_cncptn_ind',
    'temporaryAssistance': 'tanf_cash_cd',
    'healthMdeicalHome': 'hh_pgm_prtcpnt_flag',
    'healthProvider': 'hh_prvdr_num',
    'healthEntity': 'hh_ent_name',
    'malignantHyperthermia': 'mh_hh_chrnc_cond_flag',
    'substanceAbuse': 'sa_hh_chrnc_cond_flag',
    'asthma': 'asthma_hh_chrnc_cond_flag',
    'diabetes': 'dbts_hh_chrnc_cond_flag',
    'heartDisease': 'hrt_dis_hh_chrnc_cond_flag',
    'overweight': 'ovrwt_hh_chrnc_cond_flag',
    'hiv': 'hiv_aids_hh_chrnc_cond_flag',
    'otherChronicConidition': 'othr_hh_chrnc_cond_flag',
    'lockInProvider': ['lckin_prvdr_num1','lckin_prvdr_num2','lckin_prvdr_num3'],
    'lockInProviderType': ['lckin_prvdr_type_cd1','lckin_prvdr_type_cd2','lckin_prvdr_type_cd3'],    
    'lockIn': 'lckin_flag',
    'longTermProvider': ['ltss_prvdr_num1','ltss_prvdr_num2','ltss_prvdr_num3'],
    'longTermCareLevel': ['ltss_lvl_care_cd1','ltss_lvl_care_cd2','ltss_lvl_care_cd3'],
    'mc_plans' : mc_plans,
    'livesWithFamily': 'mfp_lvs_wth_fmly_cd',
    'qualifiedInstituion': 'mfp_qlfyd_instn_cd',
    'qualifiedResidence': 'mfp_qlfyd_rsdnc_cd',
    'participationEndedReason': 'mfp_prtcptn_endd_rsn_cd',
    'reinstitutionalizedReasonCode': 'mfp_rinstlzd_rsn_cd',
    'moneyFollowsPerson': 'mfp_prtcpnt_flag',
    'cfc': 'cmnty_1st_chs_spo_flag',
    '1915i': '_1915i_spo_flag',
    '1932a': '_1932a_spo_flag',
    '1915a': '_1915a_spo_flag',
    '1937': '_1937_abp_spo_flag',
    '1115a': '_1115a_prtcpnt_flag',
    'waiver': ['wvr_id1','wvr_id2','wvr_id3','wvr_id4','wvr_id5','wvr_id6','wvr_id7','wvr_id8','wvr_id9','wvr_id10'],
    'wavier': ['wvr_type_cd1','wvr_type_cd2','wvr_type_cd3','wvr_type_cd4','wvr_type_cd5','wvr_type_cd6','wvr_type_cd7','wvr_type_cd8','wvr_type_cd9','wvr_type_cd10'],
    'thirdPartyLiabilityInsurance': 'tpl_insrnc_cvrg_ind',
    'thirdPartyLiabilityOther': 'tpl_othr_cvrg_ind',
    '1115aDemo': 'sect_1115a_demo_ind',
    'nativeHawaiian': 'ntv_hi_flag',
    'guamanian': 'guam_chamorro_flag',
    'samoan': 'samoan_flag',
    'otherPacificIslander': 'othr_pac_islndr_flag',
    'unknownPacificIslander': 'unk_pac_islndr_flag',
    'asian': 'asn_indn_flag',
    'chinese': 'chinese_flag',
    'filipino': 'filipino_flag',
    'japanese': 'japanese_flag',
    'korean': 'korean_flag',
    'vietmanese': 'vietnamese_flag',
    'otherAsian': 'othr_asn_flag',
    'unknownAsian': 'unk_asn_flag',
    'white': 'wht_flag',
    'black': 'black_afrcn_amrcn_flag',
    'native': 'aian_flag',
    'race': 'race_ethncty_flag',
    'raceExpanded': 'race_ethncty_exp_flag',
    'hispanic': 'hspnc_ethncty_flag',
    'timestampAdd': 'rec_add_ts',
    'timestampUpdate': 'rec_updt_ts',
    # : 'elgbl_id_addtnl',
    # : 'elgbl_id_addtnl_ent_id',
    # : 'elgbl_id_addtnl_rsn_chg',
    # : 'elgbl_id_msis_xwalk',
    # : 'elgbl_id_msis_xwalk_ent_id',
    # : 'elgbl_id_msis_xwalk_rsn_chg',
    # : 'da_run_id',
    # : 'bsf_fil_dt',
    # : 'bsf_vrsn',
    # 'state': 'submtg_state_cd',
    'run_id' : 'tmsis_run_id'
  } 