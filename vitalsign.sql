create or replace table `saved.vitalsign` as

with grouped as 
(
select 
subject_id,
stay_id,
MIN(heart_rate) AS heart_rate_min
, MAX(heart_rate) AS heart_rate_max
, ROUND(AVG(heart_rate),2) AS heart_rate_mean
, MIN(sbp) AS sbp_min
, MAX(sbp) AS sbp_max
, ROUND(AVG(sbp),2) AS sbp_mean
, MIN(dbp) AS dbp_min
, MAX(dbp) AS dbp_max
, ROUND(AVG(dbp),2) AS dbp_mean
, MIN(resp_rate) AS resp_rate_min
, MAX(resp_rate) AS resp_rate_max
, ROUND(AVG(resp_rate),2) AS resp_rate_mean
, MIN(temperature) AS temperature_min
, MAX(temperature) AS temperature_max
, ROUND(AVG(temperature),2) AS temperature_mean
, MIN(spo2) AS spo2_min
, MAX(spo2) AS spo2_max
, ROUND(AVG(spo2),2) AS spo2_mean
, MIN(glucose) AS glucose_min
, MAX(glucose) AS glucose_max
, ROUND(AVG(glucose),2) AS glucose_mean
from `physionet-data.mimiciv_derived.vitalsign`
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
'Between ' || intime_offset || ' and ' || outtime_offset || ' hours after admission the patient stayed in the ICU and during this period had the following measurements ' ||
coalesce('minimum heart rate: '                     || CAST(heart_rate_min AS STRING), '')      ||
coalesce(', maximum heart rate: '                   || CAST(heart_rate_max AS STRING), '')      ||
coalesce(', average heart rate: '                   || CAST(heart_rate_mean AS STRING), '')     ||
coalesce(', minimum systolic blood pressure: '      || CAST(sbp_min AS STRING), '')             ||
coalesce(', maximum systolic blood pressure: '      || CAST(sbp_max AS STRING), '')             ||
coalesce(', average systolic blood pressure: '      || CAST(sbp_mean AS STRING), '')            ||
coalesce(', minimum diastolic blood pressure: '     || CAST(dbp_min AS STRING), '')             ||
coalesce(', maximum diastolic blood pressure: '     || CAST(dbp_max AS STRING), '')             ||
coalesce(', average diastolic blood pressure: '     || CAST(dbp_mean AS STRING), '')            ||
coalesce(', minimum respiration rate: '             || CAST(resp_rate_min AS STRING), '')       ||
coalesce(', maximum respiration rate: '             || CAST(resp_rate_max AS STRING), '')       ||
coalesce(', average respiration rate: '             || CAST(resp_rate_mean AS STRING), '')      ||
coalesce(', minimum temperature: '                  || CAST(temperature_min AS STRING), '')     ||
coalesce(', maximum temperature: '                  || CAST(temperature_max AS STRING), '')     ||
coalesce(', average temperature: '                  || CAST(temperature_mean AS STRING), '')    ||
coalesce(', minimum peripheral oxygen saturation: ' || CAST(spo2_min AS STRING), '')            ||
coalesce(', maximum peripheral oxygen saturation: ' || CAST(spo2_max AS STRING), '')            ||
coalesce(', average peripheral oxygen saturation: ' || CAST(spo2_mean AS STRING), '')           ||
coalesce(', minimum blood glucose level: '          || CAST(glucose_min AS STRING), '')         ||
coalesce(', maximum blood glucose level: '          || CAST(glucose_max AS STRING), '')         ||
coalesce(', average blood glucose level: '          || CAST(glucose_mean AS STRING), '') as vitalsign_report
from merged
)

select subject_id, hadm_id, count(distinct(intime_offset)) as nu_stays, string_agg(vitalsign_report, '\n' order by intime_offset) AS vitalsign_report
from concatenated
group by subject_id, hadm_id
order by nu_stays desc
