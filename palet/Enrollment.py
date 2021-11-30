from pyspark.sql import SparkSession



class Enrollment():

  by = {}
  by_group = []
  filter = {}

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
  def mc_plans():
    print('mc_plans')
 

  def byEthnicity(self):
    # print('by ethnicity...')
    self.by_group.append('ethnicity')
    return self

  def byState(self):
    # print('by state...')
    self.by_group.append('state')
    return self

  def fetch(self):
    spark = SparkSession.getActiveSession()
    # r = f"""
    #       select
    #       t.SUBMTG_STATE_CD
    #       ,count(distinct t.msis_ident_num) as m
    #     from
    #       taf.taf_mon_bsf as t
    #     inner join
    #       taf.tmp_max_da_run_id r
    #         on r.DA_RUN_ID = t.da_run_id
    #         and r.SUBMTG_STATE_CD = t.SUBMTG_STATE_CD
    #     where
    #       t.da_run_id = '1875' #Need to get latest run id
    #     group by
    #       {','.join(self.by_group)}
    #     order by
    #       {','.join(self.by_group)}
    # """
    sql = f"""
            select
                t.SUBMTG_STATE_CD
                , count(distinct t.msis_ident_num) as num_enrolls
            from
                taf.taf_mon_bsf as t
            inner join
                taf.tmp_max_da_run_id r
                on r.DA_RUN_ID = t.da_run_id
                and r.SUBMTG_STATE_CD = t.SUBMTG_STATE_CD
            where
                t.da_run_id = '1875' --Need to figure out how to get latest runId
            group by
                {','.join(self.by_group)}
            order by
                {','.join(self.by_group)}
        """
    return sql
  # ---------------------------------------------------------------------------------
  #
  #
  #
  #
  # ---------------------------------------------------------------------------------
  attributes = {
    
    'Encrypted State Assigned Beneficiary ID': 'msis_ident_num',
    'Beneficiary Social Security Number': 'ssn_num',
    'Social Security Indicator': 'ssn_ind',
    'Social Security Verification Indicator': 'ssn_vrfctn_ind',
    'Medicare Beneficiary ID': 'mdcr_bene_id',
    'Beneficiary Health Insurance Claim Number': 'mdcr_hicn_num',
    'TODO: Add attribute': 'reg_flag',
    'Encrypted TMSIS Case Number': 'msis_case_num',
    'TODO: Add attribute': 'sngl_enrlmt_flag',


    medicaid_enrollment_intervals : 
    : 'mdcd_enrlmt_eff_dt_1',
    : 'mdcd_enrlmt_end_dt_1',
    : 'mdcd_enrlmt_eff_dt_2',
    : 'mdcd_enrlmt_end_dt_2',
    : 'mdcd_enrlmt_eff_dt_3',
    : 'mdcd_enrlmt_end_dt_3',
    : 'mdcd_enrlmt_eff_dt_4',
    : 'mdcd_enrlmt_end_dt_4',
    : 'mdcd_enrlmt_eff_dt_5',
    : 'mdcd_enrlmt_end_dt_5',
    : 'mdcd_enrlmt_eff_dt_6',
    : 'mdcd_enrlmt_end_dt_6',
    : 'mdcd_enrlmt_eff_dt_7',
    : 'mdcd_enrlmt_end_dt_7',
    : 'mdcd_enrlmt_eff_dt_8',
    : 'mdcd_enrlmt_end_dt_8',
    : 'mdcd_enrlmt_eff_dt_9',
    : 'mdcd_enrlmt_end_dt_9',
    : 'mdcd_enrlmt_eff_dt_10',
    : 'mdcd_enrlmt_end_dt_10',
    : 'mdcd_enrlmt_eff_dt_11',
    : 'mdcd_enrlmt_end_dt_11',
    : 'mdcd_enrlmt_eff_dt_12',
    : 'mdcd_enrlmt_end_dt_12',
    : 'mdcd_enrlmt_eff_dt_13',
    : 'mdcd_enrlmt_end_dt_13',
    : 'mdcd_enrlmt_eff_dt_14',
    : 'mdcd_enrlmt_end_dt_14',
    : 'mdcd_enrlmt_eff_dt_15',
    : 'mdcd_enrlmt_end_dt_15',
    : 'mdcd_enrlmt_eff_dt_16',
    : 'mdcd_enrlmt_end_dt_16',


    chip_enrollment_intervals : 
    : 'chip_enrlmt_eff_dt_1',
    : 'chip_enrlmt_end_dt_1',
    : 'chip_enrlmt_eff_dt_2',
    : 'chip_enrlmt_end_dt_2',
    : 'chip_enrlmt_eff_dt_3',
    : 'chip_enrlmt_end_dt_3',
    : 'chip_enrlmt_eff_dt_4',
    : 'chip_enrlmt_end_dt_4',
    : 'chip_enrlmt_eff_dt_5',
    : 'chip_enrlmt_end_dt_5',
    : 'chip_enrlmt_eff_dt_6',
    : 'chip_enrlmt_end_dt_6',
    : 'chip_enrlmt_eff_dt_7',
    : 'chip_enrlmt_end_dt_7',
    : 'chip_enrlmt_eff_dt_8',
    : 'chip_enrlmt_end_dt_8',
    : 'chip_enrlmt_eff_dt_9',
    : 'chip_enrlmt_end_dt_9',
    : 'chip_enrlmt_eff_dt_10',
    : 'chip_enrlmt_end_dt_10',
    : 'chip_enrlmt_eff_dt_11',
    : 'chip_enrlmt_end_dt_11',
    : 'chip_enrlmt_eff_dt_12',
    : 'chip_enrlmt_end_dt_12',
    : 'chip_enrlmt_eff_dt_13',
    : 'chip_enrlmt_end_dt_13',
    : 'chip_enrlmt_eff_dt_14',
    : 'chip_enrlmt_end_dt_14',
    : 'chip_enrlmt_eff_dt_15',
    : 'chip_enrlmt_end_dt_15',
    : 'chip_enrlmt_eff_dt_16',
    : 'chip_enrlmt_end_dt_16',




    'Beneficiary First Name': 'elgbl_1st_name',
    'Beneficiary Last Name': 'elgbl_last_name',
    'Beneficiary Middle Initial': 'elgbl_mdl_initl_name',
    'dob': 'birth_dt',
    'dod': 'death_dt',
    'age': 'age_num',
    'ageGroup' : 'age_grp_flag',
    : 'dcsd_flag',
    'gender': 'gndr_cd',
    'maritalStatus': 'mrtl_stus_cd',
    'income': 'incm_cd',
    'vet': 'vet_ind',
    'citizenship': 'ctznshp_ind',
    : 'ctznshp_vrfctn_ind',
    : 'imgrtn_stus_cd',
    : 'imgrtn_vrfctn_ind',
    : 'imgrtn_stus_5_yr_bar_end_dt',
    : 'othr_lang_home_cd',
    : 'prmry_lang_flag',
    : 'prmry_lang_englsh_prfcncy_cd',
    : 'hsehld_size_cd',
    : 'prgnt_ind',
    : 'prgncy_flag',
    : 'crtfd_amrcn_indn_alskn_ntv_ind',
    : 'ethncty_cd',
    'address' : ['elgbl_line_1_adr_home','elgbl_line_2_adr_home','elgbl_line_3_adr_home'],
    : 'elgbl_city_name_home',
    : 'elgbl_zip_cd_home',
    : 'elgbl_cnty_cd_home',
    : 'elgbl_state_cd_home',
    : 'elgbl_phne_num_home',
    : 'elgbl_line_1_adr_mail',
    : 'elgbl_line_2_adr_mail',
    : 'elgbl_line_3_adr_mail',
    : 'elgbl_city_name_mail',
    : 'elgbl_zip_cd_mail',
    : 'elgbl_cnty_cd_mail',
    : 'elgbl_state_cd_mail',
    : 'care_lvl_stus_cd',
    : 'deaf_dsbl_flag',
    : 'blnd_dsbl_flag',
    : 'dfclty_conc_dsbl_flag',
    : 'dfclty_wlkg_dsbl_flag',
    : 'dfclty_drsng_bathg_dsbl_flag',
    : 'dfclty_errands_aln_dsbl_flag',
    : 'othr_dsbl_flag',
    : 'hcbs_aged_non_hhcc_flag',
    : 'hcbs_phys_dsbl_non_hhcc_flag',
    : 'hcbs_intel_dsbl_non_hhcc_flag',
    : 'hcbs_autsm_non_hhcc_flag',
    : 'hcbs_dd_non_hhcc_flag',
    : 'hcbs_mi_sed_non_hhcc_flag',
    : 'hcbs_brn_inj_non_hhcc_flag',
    : 'hcbs_hiv_aids_non_hhcc_flag',
    : 'hcbs_tech_dep_mf_non_hhcc_flag',
    : 'hcbs_dsbl_othr_non_hhcc_flag',
    : 'enrl_type_flag',
    : 'days_elig_in_mo_cnt',
    : 'elgbl_entir_mo_ind',
    : 'elgbl_last_day_of_mo_ind',
    : 'chip_cd',
    : 'elgblty_grp_cd',
    : 'prmry_elgblty_grp_ind',
    : 'elgblty_grp_ctgry_flag',
    : 'mas_cd',
    : 'elgblty_mdcd_basis_cd',
    : 'masboe_cd',
    : 'state_spec_elgblty_fctr_txt',
    : 'dual_elgbl_cd',
    : 'dual_elgbl_flag',
    : 'rstrctd_bnfts_cd',
    : 'ssdi_ind',
    : 'ssi_ind',
    : 'ssi_state_splmt_stus_cd',
    : 'ssi_stus_cd',
    : 'birth_cncptn_ind',
    : 'tanf_cash_cd',
    : 'hh_pgm_prtcpnt_flag',
    : 'hh_prvdr_num',
    : 'hh_ent_name',
    : 'mh_hh_chrnc_cond_flag',
    : 'sa_hh_chrnc_cond_flag',
    : 'asthma_hh_chrnc_cond_flag',
    : 'dbts_hh_chrnc_cond_flag',
    : 'hrt_dis_hh_chrnc_cond_flag',
    : 'ovrwt_hh_chrnc_cond_flag',
    : 'hiv_aids_hh_chrnc_cond_flag',
    : 'othr_hh_chrnc_cond_flag',
    : 'lckin_prvdr_num1',
    : 'lckin_prvdr_type_cd1',
    : 'lckin_prvdr_num2',
    : 'lckin_prvdr_type_cd2',
    : 'lckin_prvdr_num3',
    : 'lckin_prvdr_type_cd3',
    : 'lckin_flag',
    : 'ltss_prvdr_num1',
    : 'ltss_lvl_care_cd1',
    : 'ltss_prvdr_num2',
    : 'ltss_lvl_care_cd2',
    : 'ltss_prvdr_num3',
    : 'ltss_lvl_care_cd3',
    'mc_plans' : mc_plans,
    : 'mfp_lvs_wth_fmly_cd',
    : 'mfp_qlfyd_instn_cd',
    : 'mfp_qlfyd_rsdnc_cd',
    : 'mfp_prtcptn_endd_rsn_cd',
    : 'mfp_rinstlzd_rsn_cd',
    : 'mfp_prtcpnt_flag',
    : 'cmnty_1st_chs_spo_flag',
    : '_1915i_spo_flag',
    : '_1932a_spo_flag',
    : '_1915a_spo_flag',
    : '_1937_abp_spo_flag',
    : '_1115a_prtcpnt_flag',
    : 'wvr_id1',
    : 'wvr_type_cd1',
    : 'wvr_id2',
    : 'wvr_type_cd2',
    : 'wvr_id3',
    : 'wvr_type_cd3',
    : 'wvr_id4',
    : 'wvr_type_cd4',
    : 'wvr_id5',
    : 'wvr_type_cd5',
    : 'wvr_id6',
    : 'wvr_type_cd6',
    : 'wvr_id7',
    : 'wvr_type_cd7',
    : 'wvr_id8',
    : 'wvr_type_cd8',
    : 'wvr_id9',
    : 'wvr_type_cd9',
    : 'wvr_id10',
    : 'wvr_type_cd10',
    : 'tpl_insrnc_cvrg_ind',
    : 'tpl_othr_cvrg_ind',
    : 'sect_1115a_demo_ind',
    : 'ntv_hi_flag',

    : 'guam_chamorro_flag',
    : 'samoan_flag',
    : 'othr_pac_islndr_flag',
    : 'unk_pac_islndr_flag',
    : 'asn_indn_flag',
    : 'chinese_flag',
    : 'filipino_flag',
    : 'japanese_flag',
    : 'korean_flag',
    : 'vietnamese_flag',
    : 'othr_asn_flag',
    : 'unk_asn_flag',
    : 'wht_flag',
    : 'black_afrcn_amrcn_flag',
    : 'aian_flag',

    : 'race_ethncty_flag',
    : 'race_ethncty_exp_flag',

    : 'hspnc_ethncty_flag',

    : 'rec_add_ts',
    : 'rec_updt_ts',
    : 'elgbl_id_addtnl',
    : 'elgbl_id_addtnl_ent_id',
    : 'elgbl_id_addtnl_rsn_chg',
    : 'elgbl_id_msis_xwalk',
    : 'elgbl_id_msis_xwalk_ent_id',
    : 'elgbl_id_msis_xwalk_rsn_chg',
    : 'da_run_id',
    : 'bsf_fil_dt',
    : 'bsf_vrsn',
    : 'submtg_state_cd',
    run_id : 'tmsis_run_id'
  } 