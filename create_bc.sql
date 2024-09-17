create table `saved.bc` as
select *
from `physionet-data.mimiciv_derived.complete_blood_count`
where specimen_id is not null and hadm_id is not null