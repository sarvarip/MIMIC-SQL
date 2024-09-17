create or replace table `saved.icp` as

with grouped as 
(
select 
subject_id,
stay_id,
MIN(icp) AS icp_min
, MAX(icp) AS icp_max
, ROUND(AVG(icp),2) AS icp_mean
from `physionet-data.mimiciv_derived.icp`
group by subject_id, stay_id
),

merged as
(
select v.*,
d.hadm_id, DATETIME_DIFF(d.icu_intime, d.admittime, HOUR) as intime_offset, DATETIME_DIFF(d.icu_outtime, d.admittime, HOUR) as outtime_offset
from grouped v
left join `physionet-data.mimiciv_derived.icustay_detail` d
on v.stay_id = d.stay_id
),

concatenated as 
(select subject_id, hadm_id, intime_offset, outtime_offset,
'Between ' || intime_offset || ' and ' || outtime_offset || ' hours after admission the patient stayed in the ICU and during this period had the following cranial pressure measurements ' ||
coalesce('minimum intra cranial pressure: '                     || CAST(icp_min AS STRING), '')      ||
coalesce(', maximum intra cranial pressure: '                   || CAST(icp_max AS STRING), '')      ||
coalesce(', average intra cranial pressure: '                   || CAST(icp_mean AS STRING), '') as icp_report
from merged
)

select subject_id, hadm_id, count(distinct(intime_offset)) as nu_stays, string_agg(icp_report, '\n' order by intime_offset) AS icp_report
from concatenated
group by subject_id, hadm_id
order by nu_stays desc
