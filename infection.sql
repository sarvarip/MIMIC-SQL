CREATE OR REPLACE TABLE `saved.agg_infection` AS 

with merged as 
(
select b.subject_id, b.hadm_id, b.test_name, b.spec_type_desc, b.org_name, a.admittime, DATETIME_DIFF(b.charttime, a.admittime, HOUR) as offset
from `physionet-data.mimiciv_hosp.microbiologyevents` b
inner join `physionet-data.mimiciv_hosp.admissions` a
on b.hadm_id = a.hadm_id
where b.org_name is not null and b.test_name != 'voided' and b.test_name != 'Problem'
),

distinct_microbiology as
(
-- groupby here is equivalent to select distinct
-- needs distinct because there'll be repetitions of rows when different antibiotics are tested on the culture
select subject_id, hadm_id, test_name, spec_type_desc, org_name, offset
from merged
group by subject_id, hadm_id, test_name, spec_type_desc, org_name, offset
),

concatenated as 
(
select subject_id, hadm_id, offset, 
(offset || ' hours after admission the microbiology culture test ' || test_name || ' obtained via ' || spec_type_desc|| ' identified ' || org_name) as org
from distinct_microbiology
)
select subject_id, hadm_id, string_agg(org, '\n' order by offset) AS infection_report
from concatenated
group by subject_id, hadm_id