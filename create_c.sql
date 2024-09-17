create or replace table `saved.c` as

select *except(troponin_t), SAFE_CAST(troponin_t as FLOAT64) as troponin_t
from `physionet-data.mimiciv_derived.cardiac_marker`
where specimen_id is not null and hadm_id is not null