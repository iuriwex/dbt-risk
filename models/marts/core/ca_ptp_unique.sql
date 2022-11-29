--Creates a dataset of unique promise to pay entries that need to be matched
with pro_sandbox.ca_ptp_unique as (
    SELECT * FROM {{('stg_eltool__ca_ptp_unique')}}
)
select DISTINCT
       ptpid
      ,ptp_create_dt
      ,match_key
      ,ptp_first_payment_dt
      ,ptp_payment_amt
      ,ptp_payment_freq
      ,ptp_payment_type
 from pro_sandbox.ca_salesforce;