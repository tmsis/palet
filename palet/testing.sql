
select
  mon.SUBMTG_STATE_CD,
  race_ethncty_exp_flag,
  mon.BSF_FIL_DT,
  count(*) as m
from
  taf.taf_mon_bsf as mon
  inner join (
    select
      distinct SUBMTG_STATE_CD,
      BSF_FIL_DT,
      max(DA_RUN_ID) as DA_RUN_ID
    from
      taf.tmp_max_da_run_id
    where
      --BSF_FIL_DT >= 201601 and BSF_FIL_DT <= 201812
      SUBMTG_STATE_CD = '37'
    group by
      SUBMTG_STATE_CD,
      BSF_FIL_DT
    order by
      SUBMTG_STATE_CD
  ) as rid on mon.SUBMTG_STATE_CD = rid.SUBMTG_STATE_CD
  and mon.BSF_FIL_DT = rid.BSF_FIL_DT
  and mon.DA_RUN_ID = rid.DA_RUN_ID
group by
  mon.SUBMTG_STATE_CD,
  mon.race_ethncty_exp_flag,
  mon.BSF_FIL_DT
order by
  mon.SUBMTG_STATE_CD,
  mon.race_ethncty_exp_flag