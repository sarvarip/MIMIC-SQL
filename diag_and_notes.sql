CREATE OR REPLACE TABLE `saved.agg_diag_and_notes` AS 

with diag as
(
  select c.subject_id, c.hadm_id, c.seq_num, t.long_title
  from `physionet-data.mimiciv_hosp.diagnoses_icd` c
  left join `physionet-data.mimiciv_hosp.d_icd_diagnoses` t
  --left join `saved.filtered_diagnoses` t
  on c.icd_code = t.icd_code and c.icd_version = t.icd_version
),

agg_diag as 
(select subject_id, hadm_id, string_agg(concat(CAST(seq_num AS STRING), ':', long_title), '\n' order by seq_num) AS diagnoses
from diag
group by subject_id, hadm_id
),

text_time as
(
  select r.charttime, r.text, r.subject_id, r.hadm_id, a.admittime 
  from `physionet-data.mimiciv_note.radiology` r 
  left join `physionet-data.mimiciv_hosp.admissions` a
  on r.hadm_id = a.hadm_id
),

agg_text as 
(
select subject_id, hadm_id, ARRAY_AGG(STRUCT(DATETIME_DIFF(charttime, admittime, HOUR) as offset, text)) AS notes,
string_agg(concat(CAST(DATETIME_DIFF(charttime, admittime, HOUR) AS STRING), ' hours after admission the radiologist wrote: ', text), '\n' order by DATETIME_DIFF(charttime, admittime, HOUR)) AS imaging_report
from text_time
group by subject_id, hadm_id
)

select d.*, r.imaging_report, 
from agg_diag d
--need inner here because we need radiologist notes to feed into
inner join agg_text r
on d.hadm_id = r.hadm_id
--needed because after pre-filtering we end up with some patients with zero diagnosable conditions
where diagnoses is not null