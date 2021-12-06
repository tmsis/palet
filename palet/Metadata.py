
# -------------------------------------------------------------------------------------------------
#
# -------------------------------------------------------------------------------------------------
from numpy import float64, int64


class DQM_Metadata:

    # -------------------------------------------------------------------------------------------------
    #   Eligibility Tables
    # -------------------------------------------------------------------------------------------------
    class elig_tables():

        # -------------------------------------------------------------------------------------------------
        #   Eligibility - Current
        # -------------------------------------------------------------------------------------------------
        class current():

            tblList = ('tmsis_prmry_dmgrphc_elgblty', 'tmsis_var_dmgrphc_elgblty', 'tmsis_elgbl_cntct', 'tmsis_elgblty_dtrmnt',
                       'tmsis_hh_sntrn_prtcptn_info', 'tmsis_hh_chrnc_cond', 'tmsis_lckin_info', 'tmsis_mfp_info', 'tmsis_ltss_prtcptn_data',
                       'tmsis_state_plan_prtcptn', 'tmsis_wvr_prtcptn_data', 'tmsis_mc_prtcptn_data', 'tmsis_ethncty_info', 'tmsis_race_info',
                       'tmsis_dsblty_info', 'tmsis_sect_1115a_demo_info', 'tmsis_hcbs_chrnc_cond_non_hh', 'tmsis_enrlmt_time_sgmt_data')

            dtPrefix = ('prmry_dmgrphc_ele', 'var_dmgrphc_ele', 'elgbl_adr', 'elgblty_dtrmnt',
                        'hh_sntrn_prtcptn', 'hh_chrnc', 'lckin', 'mfp_enrlmt', 'ltss_elgblty',
                        'state_plan_optn', 'wvr_enrlmt', 'mc_plan_enrlmt', 'ethncty_dclrtn', 'race_dclrtn',
                        'dsblty_type', 'sect_1115a_demo', 'ndc_uom_chrnc_non_hh', 'enrlmt')

        # -------------------------------------------------------------------------------------------------
        #   Eligibility - Prior
        # -------------------------------------------------------------------------------------------------
        class prior():

            tblList = ('tmsis_prmry_dmgrphc_elgblty', 'tmsis_var_dmgrphc_elgblty', 'tmsis_elgbl_cntct', 'tmsis_mc_prtcptn_data', 'tmsis_ethncty_info', 'tmsis_race_info')

            dtPrefix = ('prmry_dmgrphc_ele', 'var_dmgrphc_ele', 'elgbl_adr', 'mc_plan_enrlmt', 'ethncty_dclrtn', 'race_dclrtn')

    # -------------------------------------------------------------------------------------------------
    #   Provider Tables
    # -------------------------------------------------------------------------------------------------
    class prov_tables():

        tblList = ('tmsis_prvdr_attr_mn', 'tmsis_prvdr_lctn_cntct', 'tmsis_prvdr_id', 'tmsis_prvdr_txnmy_clsfctn', 'tmsis_prvdr_mdcd_enrlmt', 'tmsis_prvdr_afltd_pgm')

        dtPrefix = ('prvdr_attr', 'prvdr_lctn_cntct', 'prvdr_id', 'prvdr_txnmy_clsfctn', 'prvdr_mdcd', 'prvdr_afltd_pgm')

        # -------------------------------------------------------------------------------------------------
        #   Provider - Ever
        # -------------------------------------------------------------------------------------------------
        class ever():

            tblList = ('tmsis_prvdr_attr_mn', 'tmsis_prvdr_id', 'tmsis_prvdr_mdcd_enrlmt')

            evrvarList = ('ever_provider', 'ever_provider_id', 'ever_enrolled_provider')


        class prvdr_pct_sql():

            tblList = ('prvdr_prep', 'all_clms_prvdrs', 'uniq_clms_prvdrs_file', 'uniq_clms_prvdrs', 'prv_clm',
                       'clm_prv_tab', 'clm_prv_ip', 'clm_prv_tab_ip', 'clm_prv_lt', 'clm_prv_tab_lt',
                       'clm_prv_ot', 'clm_prv_tab_ot', 'clm_prv_rx', 'clm_prv_tab_rx', 'prv_addtyp_prep',
                       'prv_addtyp_rollup', 'prv_addtyp', 'prv_idtyp_prep', 'prv_idtyp', 'prv_mdcd_prep', 'prv_mdcd',
                       'prv_id_npi', 'prvdr_npi_txnmy', 'prvdr_npi_txnmy2',
                       'prv2_10_denom', 'prv2_10_numer', 'prv2_10_msr')

        class prvdr_freq_sql():

            tblList = ('prvdr_txnmy', 'prvdr_freq_t', 'prvdr_freq_t2')

    # -------------------------------------------------------------------------------------------------
    #   TPL Tables
    # -------------------------------------------------------------------------------------------------
    class tpl_tables():

        tblList = ('tmsis_tpl_mdcd_prsn_mn', 'tmsis_tpl_mdcd_prsn_hi')

        dtPrefix = ('elgbl_prsn_mn', 'insrnc_cvrg')

        class tpl_prsn_hi_sql():

            tpl_cvrg_typ = ('01','02','03','04','05','06','07','08','09','10',
                            '11','12','13','14','15','16','17','18','19','20',
                            '21','22','23','98')

            tpl_insrnc_typ = ('01','02','03','04','05','06','07','08','09',
                              '10','11','12','13','14','15','16')

    # -------------------------------------------------------------------------------------------------
    #   MCPlan Tables
    # -------------------------------------------------------------------------------------------------
    class mcplan_tables():

        tblList = ('tmsis_mc_mn_data', 'tmsis_mc_oprtg_authrty', 'tmsis_natl_hc_ent_id_info')

        dtPrefix = ('mc_mn_rec', 'mc_op_authrty', 'natl_hlth_care_ent_id')


        # -------------------------------------------------------------------------------------------------
        #
        # -------------------------------------------------------------------------------------------------
        class base_mc_view_columns():
            select = {
                'tmsis_mc_mn_data'          : ',mc_plan_type_cd, mc_pgm_cd, reimbrsmt_arngmt_cd',
                'tmsis_mc_lctn_cntct'       : ',mc_adr_type_cd, mc_lctn_id, rec_num',
                'tmsis_mc_sarea'            : ',mc_sarea_name',
                'tmsis_mc_oprtg_authrty'    : ',oprtg_authrty_cd, wvr_id',
                'tmsis_mc_plan_pop_enrld'   : ',mc_plan_pop_cnt',
                'tmsis_mc_acrdtn_org'       : ',acrdtn_org_cd',
                'tmsis_natl_hc_ent_id_info' : ',natl_hlth_care_ent_id, natl_hlth_care_ent_id_type_cd',
                'tmsis_chpid_shpid_rltnshp_data': ''
            }


    class create_base_elig_info_view():
        select = {
            'tmsis_prmry_dmgrphc_elgblty' :
                """,gndr_cd
                    ,death_dt
                    ,birth_dt""",
            'tmsis_var_dmgrphc_elgblty' :
                """,ssn_num
                    ,ssn_vrfctn_ind
                    ,ctznshp_ind
                    ,ctznshp_vrfctn_ind
                    ,imgrtn_vrfctn_ind
                    ,imgrtn_stus_cd
                    ,hsehld_size_cd
                    ,incm_cd
                    ,mrtl_stus_cd
                    ,vet_ind
                    ,chip_cd""",
            'tmsis_elgbl_cntct' :
                """,elgbl_state_cd
                    ,elgbl_cnty_cd
                    ,elgbl_zip_cd
                    ,elgbl_adr_type_cd""",
            'tmsis_elgblty_dtrmnt' :
                """,msis_case_num
                    ,elgblty_grp_cd
                    ,elgblty_mdcd_basis_cd
                    ,prmry_elgblty_grp_ind
                    ,dual_elgbl_cd
                    ,rstrctd_bnfts_cd
                    ,mas_cd
                    ,ssdi_ind as ssdi_ind
                    ,ssi_ind as ssi_ind
                    ,ssi_state_splmt_stus_cd
                    ,tanf_cash_cd""",
            'tmsis_hh_sntrn_prtcptn_info' :
                """,hh_ent_name
                    ,hh_sntrn_name""",
            'tmsis_hh_sntrn_prvdr' :
                """,hh_ent_name
                    ,hh_prvdr_num
                    ,hh_sntrn_name""",
            'tmsis_hh_chrnc_cond' :
                """,hh_chrnc_cd
                    ,hh_chrnc_othr_explntn_txt""",
            'tmsis_lckin_info' :
                """,lckin_prvdr_type_cd
                    ,lckin_prvdr_num""",
            'tmsis_mfp_info' :
                """""",
            'tmsis_state_plan_prtcptn'       :
                """,state_plan_optn_type_cd""",
            'tmsis_wvr_prtcptn_data' :
                """,wvr_type_cd
                ,wvr_id""",
            'tmsis_ltss_prtcptn_data' :
                """,ltss_lvl_care_cd
                    ,ltss_prvdr_num""",
            'tmsis_mc_prtcptn_data' :
                """,enrld_mc_plan_type_cd
                    ,mc_plan_id""",
            'tmsis_ethncty_info' :
                """,ethncty_cd""",
            'tmsis_race_info' :
                """,race_cd
                    ,race_othr_txt
                    ,crtfd_amrcn_indn_alskn_ntv_ind""",
            'tmsis_dsblty_info' :
                """,dsblty_type_cd""",
            'tmsis_sect_1115a_demo_info' :
                """,sect_1115a_demo_ind""",
            'tmsis_hcbs_chrnc_cond_non_hh' :
                """,ndc_uom_chrnc_non_hh_cd""",
            'tmsis_enrlmt_time_sgmt_data' :
                """,enrlmt_type_cd"""
        }

    class create_base_prov_info_view():
        select = {
            'tmsis_prvdr_attr_mn' :
                """,fac_grp_indvdl_cd
                    ,birth_dt
                    ,death_dt
                    ,prvdr_dba_name
                    ,prvdr_1st_name
                    ,prvdr_last_name
                    ,prvdr_lgl_name
                    ,prvdr_org_name""",
            'tmsis_prvdr_lctn_cntct' :
                """,adr_city_name
                    ,adr_cnty_cd
                    ,email_adr
                    ,adr_line_1_txt
                    ,adr_state_cd
                    ,prvdr_adr_type_cd
                    ,adr_zip_cd
                    ,prvdr_lctn_id
                    ,rec_num""",
            'tmsis_prvdr_lcnsg' :
                """,lcns_issg_ent_id_txt
                    ,lcns_or_acrdtn_num
                    ,lcns_type_cd
                    ,prvdr_lctn_id""",
            'tmsis_prvdr_id' :
                """,prvdr_id
                    ,prvdr_id_issg_ent_id_txt
                    ,prvdr_id_type_cd
                    ,prvdr_lctn_id""",
            'tmsis_prvdr_txnmy_clsfctn' :
                """,prvdr_clsfctn_cd
                    ,prvdr_clsfctn_type_cd""",
            'tmsis_prvdr_mdcd_enrlmt' :
                """,prvdr_mdcd_enrlmt_stus_cd
                    ,state_plan_enrlmt_cd""",
            'tmsis_prvdr_afltd_grp' :
                """,submtg_state_afltd_prvdr_id""",
            'tmsis_prvdr_afltd_pgm' :
                """,afltd_pgm_id
                    ,afltd_pgm_type_cd""",
            'tmsis_prvdr_bed_type' :
                """,bed_type_cd
                    ,prvdr_lctn_id
                    ,rec_num"""
        }

        class create_base_prov_info_view():
            class b():

                select = {
                    'ip':
                        """,b.cms_64_fed_reimbrsmt_ctgry_cd
                            ,b.srvc_endg_dt
                            ,b.stc_cd
                            ,b.rev_cd
                            ,b.prscrbng_prvdr_npi_num
                            ,b.bnft_type_cd
                            ,b.srvcng_prvdr_num
                            ,b.prvdr_fac_type_cd
                            ,b.rev_chrg_amt
                            ,b.srvcng_prvdr_spclty_cd
                            ,b.srvcng_prvdr_type_cd
                            ,b.srvc_bgnng_dt
                            ,b.alowd_amt
                            ,oprtg_prvdr_npi_num""",
                    'lt':
                        """,b.stc_cd
                            ,b.bnft_type_cd
                            ,b.srvcng_prvdr_num
                            ,b.cms_64_fed_reimbrsmt_ctgry_cd
                            ,b.srvc_bgnng_dt
                            ,b.srvc_endg_dt
                            ,b.prvdr_fac_type_cd
                            ,b.rev_chrg_amt
                            ,b.rev_cd
                            ,b.prscrbng_prvdr_npi_num
                            ,b.srvcng_prvdr_spclty_cd
                            ,b.srvcng_prvdr_type_cd
                            ,b.alowd_amt""",
                    'ot':
                        """,b.cms_64_fed_reimbrsmt_ctgry_cd
                            ,b.othr_toc_rx_clm_actl_qty
                            ,b.srvc_bgnng_dt
                            ,b.srvc_endg_dt
                            ,b.prcdr_cd
                            ,b.prcdr_cd_ind
                            ,b.stc_cd
                            ,b.rev_cd
                            ,b.hcpcs_rate
                            ,b.srvcng_prvdr_num
                            ,b.srvcng_prvdr_spclty_cd
                            ,b.prscrbng_prvdr_npi_num
                            ,b.srvcng_prvdr_txnmy_cd
                            ,b.bill_amt
                            ,b.hcpcs_srvc_cd
                            ,b.hcpcs_txnmy_cd
                            ,b.bnft_type_cd
                            ,b.copay_amt
                            ,b.mdcr_pd_amt
                            ,b.othr_insrnc_amt
                            ,b.prcdr_1_mdfr_cd
                            ,b.prcdr_2_mdfr_cd
                            ,b.srvcng_prvdr_type_cd
                            ,b.tpl_amt
                            ,b.alowd_amt""",
                    'rx':
                        """,b.cms_64_fed_reimbrsmt_ctgry_cd
                            ,b.suply_days_cnt
                            ,b.othr_toc_rx_clm_actl_qty
                            ,b.ndc_cd
                            ,b.stc_cd
                            ,b.alowd_amt
                            ,b.bill_amt
                            ,b.brnd_gnrc_ind
                            ,b.copay_amt
                            ,b.dspns_fee_amt
                            ,b.mdcr_pd_amt
                            ,b.new_refl_ind
                            ,b.othr_insrnc_amt
                            ,b.rebt_elgbl_ind
                            ,b.tpl_amt"""
                }
            class a():

                select = {
                    'ip' :
                        """,a.blg_prvdr_npi_num
                            ,a.prvdr_lctn_id
                            ,a.hosp_type_cd
                            ,a.admsn_dt""",
                    'lt':
                        """,a.nrsng_fac_days_cnt
                            ,a.mdcd_cvrd_ip_days_cnt
                            ,a.icf_iid_days_cnt
                            ,a.lve_days_cnt""",
                    'ot':
                        """,a.srvc_plc_cd
                            ,a.dgns_1_cd
                            ,a.plan_id_num
                            ,a.blg_prvdr_npi_num
                            ,a.prvdr_lctn_id
                            ,a.othr_insrnc_ind
                            ,a.othr_tpl_clctn_cd
                            ,a.pgm_type_cd
                            ,a.bill_type_cd """,
                    'rx':
                        """,a.prscrbng_prvdr_num
                            ,a.dspnsng_pd_prvdr_num"""
                }
    class create_base_cll_view():

            select = {
                'ip':
                    """,cms_64_fed_reimbrsmt_ctgry_cd
                        ,srvc_endg_dt
                        ,stc_cd
                        ,rev_cd
                        ,prscrbng_prvdr_npi_num
                        ,bnft_type_cd
                        ,srvcng_prvdr_num
                        ,prvdr_fac_type_cd
                        ,rev_chrg_amt
                        ,srvcng_prvdr_spclty_cd
                        ,srvcng_prvdr_type_cd
                        ,srvc_bgnng_dt
                        ,alowd_amt
                        ,oprtg_prvdr_npi_num""",
                'lt':
                    """,stc_cd
                        ,bnft_type_cd
                        ,srvcng_prvdr_num
                        ,cms_64_fed_reimbrsmt_ctgry_cd
                        ,srvc_bgnng_dt
                        ,srvc_endg_dt
                        ,prvdr_fac_type_cd
                        ,rev_chrg_amt
                        ,rev_cd
                        ,prscrbng_prvdr_npi_num
                        ,srvcng_prvdr_spclty_cd
                        ,srvcng_prvdr_type_cd
                        ,alowd_amt""",
                'ot':
                    """,cms_64_fed_reimbrsmt_ctgry_cd
                        ,othr_toc_rx_clm_actl_qty
                        ,srvc_bgnng_dt
                        ,srvc_endg_dt
                        ,prcdr_cd
                        ,prcdr_cd_ind
                        ,stc_cd
                        ,rev_cd
                        ,hcpcs_rate
                        ,srvcng_prvdr_num
                        ,srvcng_prvdr_spclty_cd
                        ,prscrbng_prvdr_npi_num
                        ,srvcng_prvdr_txnmy_cd
                        ,bill_amt
                        ,hcpcs_srvc_cd
                        ,hcpcs_txnmy_cd
                        ,bnft_type_cd
                        ,copay_amt
                        ,mdcr_pd_amt
                        ,othr_insrnc_amt
                        ,prcdr_1_mdfr_cd
                        ,prcdr_2_mdfr_cd
                        ,srvcng_prvdr_type_cd
                        ,tpl_amt
                        ,alowd_amt""",
                'rx':
                    """,cms_64_fed_reimbrsmt_ctgry_cd
                        ,suply_days_cnt
                        ,othr_toc_rx_clm_actl_qty
                        ,ndc_cd
                        ,stc_cd
                        ,alowd_amt
                        ,bill_amt
                        ,brnd_gnrc_ind
                        ,copay_amt
                        ,dspns_fee_amt
                        ,mdcr_pd_amt
                        ,new_refl_ind
                        ,othr_insrnc_amt
                        ,rebt_elgbl_ind
                        ,tpl_amt"""
            }

    class create_base_clh_view():

        select = {
            'ip':
                """,admsn_dt
                    ,admsn_type_cd
                    ,blg_prvdr_type_cd
                    ,dgns_poa_1_cd_ind
                    ,dschrg_dt
                    ,fixd_pymt_ind
                    ,hlth_care_acqrd_cond_cd
                    ,mdcd_dsh_pd_amt
                    ,mdcd_cvrd_ip_days_cnt
                    ,mdcr_pd_amt
                    ,mdcr_reimbrsmt_type_cd
                    ,ncvrd_chrgs_amt
                    ,prcdr_1_cd_dt
                    ,prcdr_2_cd_dt
                    ,prcdr_1_cd_ind
                    ,prcdr_2_cd_ind
                    ,pgm_type_cd
                    ,tot_alowd_amt
                    ,tot_copay_amt
                    ,tot_othr_insrnc_amt
                    ,tot_tpl_amt
                    ,bill_type_cd
                    ,ptnt_stus_cd
                    ,drg_cd
                    ,drg_cd_ind
                    ,dgns_1_cd
                    ,dgns_2_cd
                    ,dgns_3_cd
                    ,dgns_4_cd
                    ,dgns_5_cd
                    ,dgns_6_cd
                    ,dgns_7_cd
                    ,dgns_8_cd
                    ,dgns_9_cd
                    ,dgns_10_cd
                    ,dgns_11_cd
                    ,dgns_12_cd
                    ,prcdr_1_cd
                    ,prcdr_2_cd
                    ,prcdr_3_cd
                    ,prcdr_4_cd
                    ,prcdr_5_cd
                    ,prcdr_6_cd
                    ,prvdr_lctn_id
                    ,blg_prvdr_npi_num
                    ,hosp_type_cd
                    ,tot_mdcr_coinsrnc_amt
                    ,tot_mdcr_ddctbl_amt
                    ,pymt_lvl_ind
                    ,admtg_prvdr_npi_num
                    ,admtg_prvdr_num
                    ,rfrg_prvdr_npi_num
                    ,rfrg_prvdr_num""",
            'lt':
                """,nrsng_fac_days_cnt
                    ,mdcd_cvrd_ip_days_cnt
                    ,icf_iid_days_cnt
                    ,lve_days_cnt
                    ,ptnt_stus_cd
                    ,srvc_endg_dt
                    ,ltc_rcp_lblty_amt
                    ,dgns_1_cd
                    ,dgns_2_cd
                    ,dgns_3_cd
                    ,dgns_4_cd
                    ,dgns_5_cd
                    ,prvdr_lctn_id
                    ,blg_prvdr_npi_num
                    ,srvc_bgnng_dt
                    ,blg_prvdr_type_cd
                    ,dgns_1_cd_ind
                    ,dgns_2_cd_ind
                    ,dgns_poa_1_cd_ind
                    ,fixd_pymt_ind
                    ,hlth_care_acqrd_cond_cd
                    ,mdcr_pd_amt
                    ,mdcr_reimbrsmt_type_cd
                    ,pgm_type_cd
                    ,tot_alowd_amt
                    ,tot_mdcr_coinsrnc_amt
                    ,tot_mdcr_ddctbl_amt
                    ,tot_othr_insrnc_amt
                    ,tot_tpl_amt
                    ,bill_type_cd
                    ,pymt_lvl_ind
                    ,admtg_prvdr_npi_num
                    ,admtg_prvdr_num
                    ,rfrg_prvdr_npi_num
                    ,rfrg_prvdr_num""",
            'ot':
                """,dgns_1_cd
                    ,dgns_2_cd
                    ,srvc_plc_cd
                    ,prvdr_lctn_id
                    ,blg_prvdr_npi_num
                    ,srvc_bgnng_dt
                    ,blg_prvdr_type_cd
                    ,dgns_1_cd_ind
                    ,dgns_2_cd_ind
                    ,dgns_poa_1_cd_ind
                    ,srvc_endg_dt
                    ,fixd_pymt_ind
                    ,hh_prvdr_ind
                    ,pgm_type_cd
                    ,tot_mdcr_coinsrnc_amt
                    ,tot_mdcr_ddctbl_amt
                    ,tot_othr_insrnc_amt
                    ,tot_tpl_amt
                    ,bill_type_cd
                    ,tot_alowd_amt
                    ,pymt_lvl_ind
                    ,rfrg_prvdr_npi_num
                    ,rfrg_prvdr_num""",
            'rx':
                """,prscrbd_dt
                    ,rx_fill_dt
                    ,prvdr_lctn_id
                    ,blg_prvdr_npi_num
                    ,dspnsng_pd_prvdr_npi_num
                    ,dspnsng_pd_prvdr_num
                    ,prscrbng_prvdr_num
                    ,tot_mdcr_coinsrnc_amt
                    ,tot_mdcr_ddctbl_amt
                    ,tot_othr_insrnc_amt
                    ,tot_tpl_amt
                    ,cmpnd_drug_ind
                    ,fixd_pymt_ind
                    ,pymt_lvl_ind
                    ,srvcng_prvdr_npi_num
                    ,pgm_type_cd
                    ,tot_alowd_amt
                    ,tot_copay_amt"""
        }

        claim_cat = {
             'A': "(clm_type_cd = '1' and adjstmt_ind = '0' and (xovr_ind = '0' or xovr_ind is null))"
            ,'B': "(clm_type_cd = '1' and adjstmt_ind = '0' and xovr_ind = '1')"
            ,'C': "(clm_type_cd = '1')"
            ,'D': "(clm_type_cd = '2' and adjstmt_ind = '0')"
            ,'E': "(clm_type_cd = '2')"
            ,'F': "(clm_type_cd = 'A' and adjstmt_ind = '0' and (xovr_ind = '0' or xovr_ind is null))"
            ,'G': "(clm_type_cd = 'A' and adjstmt_ind = '0' and xovr_ind = '1')"
            ,'H': "(clm_type_cd = 'A' and adjstmt_ind = '0')"
            ,'I': "(clm_type_cd = 'A')"
            ,'J': "(clm_type_cd = 'B' and adjstmt_ind = '0')"
            ,'K': "(clm_type_cd = 'B')"
            ,'L': "(clm_type_cd in ('1','3') and adjstmt_ind = '0' and (xovr_ind = '0' or xovr_ind is null))"
            ,'M': "(clm_type_cd = '1' and adjstmt_ind = '0')"
            ,'N': "(clm_type_cd in ('1','3','A','C') and adjstmt_ind = '0' and (xovr_ind = '0' or xovr_ind is null))"
            ,'O': "(clm_type_cd = '3')"
            ,'P': "(clm_type_cd = '3' and adjstmt_ind = '0' and (xovr_ind = '0' or xovr_ind is null))"
            ,'Q': "(clm_type_cd = '3' and adjstmt_ind = '0')"
            ,'R': "(clm_type_cd = 'C' and adjstmt_ind = '0' and (xovr_ind = '0' or xovr_ind is null))"
            ,'S': "(clm_type_cd = 'C' and adjstmt_ind = '0')"
            ,'T': "(clm_type_cd = '3' and adjstmt_ind = '0' and xovr_ind = '1')"
            ,'U': "(clm_type_cd = 'C')"
            ,'V': "(clm_type_cd = 'C' and adjstmt_ind = '0' and xovr_ind = '1')"
            ,'W': "(1=1)"
            ,'X': "(clm_type_cd in ('2','B') and adjstmt_ind = '0')"
            ,'Y': "(clm_type_cd = '2')"
            ,'Z': "(clm_type_cd = 'B')"
            ,'AA': "(clm_type_cd in ('1','3', 'A','C') and adjstmt_ind in ('0','4') )"
            ,'AB': "(clm_type_cd in ('1','3') and adjstmt_ind in ('0') )"
            ,'AC': "(clm_type_cd in ('A','C') and adjstmt_ind in ('0') )"
            ,'AD': "(clm_type_cd in ('1') and xovr_ind = '1' )"
            ,'AE': "(clm_type_cd in ('1','A') and adjstmt_ind in ('0') )"
            ,'AF': "(clm_type_cd in ('3','C') and adjstmt_ind in ('0') )"
            ,'AG': "(clm_type_cd in ('2','B') and adjstmt_ind in ('0','4') )"
            ,'AH': "(clm_type_cd in ('1','A') and adjstmt_ind in ('0','4') )"
            ,'AI': "(clm_type_cd in ('1','3') and xovr_ind = '1' )"
            ,'AJ': "(clm_type_cd in ('1','3','A','C') )"
            ,'AK': "(clm_type_cd in ('1','A'))"
            ,'AL': "(clm_type_cd in ('3','C'))"
            ,'AM': "(clm_type_cd in ('1') and adjstmt_ind in ('0','4') )"
            ,'AN': "(clm_type_cd in ('A') and adjstmt_ind in ('0','4') )"
            ,'AO': "(clm_type_cd in ('1','A') and (xovr_ind = '0' or xovr_ind is null))"
            ,'AP': "(clm_type_cd in ('3','C') and (xovr_ind = '0' or xovr_ind is null))"
            ,'AQ': "(clm_type_cd in ('1','A') and xovr_ind = '1' )"
            ,'AR': "(clm_type_cd in ('3','C') and xovr_ind = '1' )"
            ,'AS': "(clm_type_cd in ('4','D') and adjstmt_ind <> '1' )"
            ,'AT': "(clm_type_cd in ('5','E') )"
            ,'AU': "(clm_type_cd in ('4','D') )"
            ,'AV': "(clm_type_cd in ('4') and adjstmt_ind <> '1' )"
            ,'AW': "(clm_type_cd in ('D') and adjstmt_ind <> '1' )"
        }

    class create_claims_tables():

        class b():
            select = {
                'ip':
                    """,b.cms_64_fed_reimbrsmt_ctgry_cd
                        ,b.srvc_endg_dt
                        ,b.stc_cd
                        ,b.rev_cd
                        ,b.prscrbng_prvdr_npi_num
                        ,b.bnft_type_cd
                        ,b.srvcng_prvdr_num
                        ,b.prvdr_fac_type_cd
                        ,b.rev_chrg_amt
                        ,b.srvcng_prvdr_spclty_cd
                        ,b.srvcng_prvdr_type_cd
                        ,b.srvc_bgnng_dt
                        ,b.alowd_amt
                        ,oprtg_prvdr_npi_num""",
                'lt':
                    """,b.stc_cd
                        ,b.bnft_type_cd
                        ,b.srvcng_prvdr_num
                        ,b.cms_64_fed_reimbrsmt_ctgry_cd
                        ,b.srvc_bgnng_dt
                        ,b.srvc_endg_dt
                        ,b.prvdr_fac_type_cd
                        ,b.rev_chrg_amt
                        ,b.rev_cd
                        ,b.prscrbng_prvdr_npi_num
                        ,b.srvcng_prvdr_spclty_cd
                        ,b.srvcng_prvdr_type_cd
                        ,b.alowd_amt""",
                'ot':
                    """,b.cms_64_fed_reimbrsmt_ctgry_cd
                        ,b.othr_toc_rx_clm_actl_qty
                        ,b.srvc_bgnng_dt
                        ,b.srvc_endg_dt
                        ,b.prcdr_cd
                        ,b.prcdr_cd_ind
                        ,b.stc_cd
                        ,b.rev_cd
                        ,b.hcpcs_rate
                        ,b.srvcng_prvdr_num
                        ,b.srvcng_prvdr_spclty_cd
                        ,b.prscrbng_prvdr_npi_num
                        ,b.srvcng_prvdr_txnmy_cd
                        ,b.bill_amt
                        ,b.hcpcs_srvc_cd
                        ,b.hcpcs_txnmy_cd
                        ,b.bnft_type_cd
                        ,b.copay_amt
                        ,b.mdcr_pd_amt
                        ,b.othr_insrnc_amt
                        ,b.prcdr_1_mdfr_cd
                        ,b.prcdr_2_mdfr_cd
                        ,b.srvcng_prvdr_type_cd
                        ,b.tpl_amt
                        ,b.alowd_amt""",
                'rx':
                    """,b.cms_64_fed_reimbrsmt_ctgry_cd
                        ,b.suply_days_cnt
                        ,b.othr_toc_rx_clm_actl_qty
                        ,b.ndc_cd
                        ,b.stc_cd
                        ,b.alowd_amt
                        ,b.bill_amt
                        ,b.brnd_gnrc_ind
                        ,b.copay_amt
                        ,b.dspns_fee_amt
                        ,b.mdcr_pd_amt
                        ,b.new_refl_ind
                        ,b.othr_insrnc_amt
                        ,b.rebt_elgbl_ind
                        ,b.tpl_amt"""
            }
        class a():

                select = {
                    'ip' :
                        """,a.blg_prvdr_npi_num
                            ,a.prvdr_lctn_id
                            ,a.hosp_type_cd
                            ,a.admsn_dt""",
                    'lt':
                        """,a.nrsng_fac_days_cnt
                            ,a.mdcd_cvrd_ip_days_cnt
                            ,a.icf_iid_days_cnt
                            ,a.lve_days_cnt""",
                    'ot':
                        """,a.srvc_plc_cd
                            ,a.dgns_1_cd
                            ,a.plan_id_num
                            ,a.blg_prvdr_npi_num
                            ,a.prvdr_lctn_id
                            ,a.othr_insrnc_ind
                            ,a.othr_tpl_clctn_cd
                            ,a.pgm_type_cd
                            ,a.bill_type_cd""",
                    'rx':
                        """,a.prscrbng_prvdr_num
                            ,a.dspnsng_pd_prvdr_num"""
                }

    # -------------------------------------------------------------------------------------------------
    #   Missingness - non claims pct
    # -------------------------------------------------------------------------------------------------
    class Missingness():
        class non_claims_pct():

            group_by = {
                'ELG': "group by msis_ident_num",
                'MCR': "group by state_plan_id_num",
                'PRV': "group by submtg_state_prvdr_id",
                'TPL': "group by msis_ident_num"
            }



    # -------------------------------------------------------------------------------------------------
    #   Reports
    # -------------------------------------------------------------------------------------------------
    class Reports():
        class waiver():

            columns = [
                'waiver_id',
                'waiver_type',
                'statistic_type',
                'Measure_ID',
                'Statistic',
                'Report_State',
                'Month_Added',
                'Statistic_Year_Month',
                'SpecVersion',
                'RunID']

            types = {
                'waiver_id': str,
                'waiver_type': str,
                'statistic_type': str,
                'Measure_ID': str,
                'Statistic': str,
                'Report_State': str,
                'Month_Added': str,
                'Statistic_Year_Month': str,
                'SpecVersion': str,
                'RunID': str
            }
        class plan8_2():

            columns = [
                'plan_id',
                'plan_type_el',
                'MultiplePlanTypes_el',
                'plan_type_mc',
                'MultiplePlanTypes_mc',
                'In_MCR_File',
                'statistic_type',
                'Measure_ID',
                'Statistic',
                'Report_State',
                'Month_Added',
                'Statistic_Year_Month',
                'SpecVersion',
                'RunID']

            id_vars = ['plan_id','plan_type_el','MultiplePlanTypes_el','plan_type_mc','MultiplePlanTypes_mc','linked','Measure_ID','Report_State','Month_Added','Statistic_Year_Month','SpecVersion','RunID']

            value_vars = ['cap_hmo','cap_php','cap_pccm','cap_phi','cap_oth','cap_tot','cap_ratio','enc_ip','enc_lt','enc_ot','enc_rx','ip_ratio','lt_ratio','ot_ratio','rx_ratio']

            statistic_type_formats = {
                'cap_hmo' : 'HMO capitation',
                'cap_php' : 'PHP capitation',
                'cap_pccm' : 'PCCM capitation',
                'cap_phi' : 'PHI capitation',
                'cap_oth' : 'Other capitation',
                'cap_tot' : 'Total capitation',
                'cap_ratio' : 'Capitation Ratio',
                'enc_ip' : 'IP encounters',
                'enc_lt' : 'LT encounters',
                'enc_ot' : 'OT encounters',
                'enc_rx' : 'RX encounters',
                'ip_ratio' : 'IP Ratio',
                'lt_ratio' : 'LT Ratio',
                'ot_ratio' : 'OT Ratio',
                'rx_ratio' : 'RX Ratio'
            }

            types = {
                'plan_id': str,
                'plan_type_el': str,
                'MultiplePlanTypes_el': str,
                'plan_type_mc': str,
                'MultiplePlanTypes_mc': str,
                'In_MCR_File': str,
                'statistic_type': str,
                'Measure_ID': str,
                'Statistic': str,
                'Report_State': str,
                'Month_Added': str,
                'Statistic_Year_Month': str,
                'SpecVersion': str,
                'RunID': str
            }

        class plan9_1():

            columns = [
                'plan_id',
                'plan_type_el',
                'statistic_type',
                'Measure_ID',
                'Statistic',
                'Report_State',
                'Month_Added',
                'Statistic_Year_Month',
                'SpecVersion',
                'RunID']

            types = {
                'plan_id': str,
                'plan_type_el': str,
                'statistic_type': str,
                'Measure_ID': str,
                'Statistic': str,
                'Report_State': str,
                'Month_Added': str,
                'Statistic_Year_Month': str,
                'SpecVersion': str,
                'RunID': str
            }

        class summary():

            columns = [
                'Report_State',
                'Month_Added',
                'Measure_ID',
                'Statistic_Year_Month',
                'Statistic',
                'Numerator',
                'Denominator',
                'valid_value',
                'SpecVersion',
                'RunID',
                'Measure_Type',
                'Active_Ind',
                'Display_Type',
                'Calculation_Source',
                'in_measures',
                'in_thresholds',
                'numer',
                'denom',
                'claim_type']

            types = {
                'Report_State': str,
                'Month_Added': str,
                'Measure_ID': str,
                'Statistic_Year_Month': str,
                'Statistic': str,
                'Numerator': str,
                'Denominator': str,
                'valid_value': str,
                'SpecVersion': str,
                'RunID': str,
                'Measure_Type': str,
                'Active_Ind': str,
                'Display_Type': str,
                'Calculation_Source': str,
                'in_measures': int64,
                'in_thresholds': int64,
                'numer': float64,
                'denom': float64,
                'claim_type': str
            }