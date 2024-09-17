create table `saved.crp` as
select *
from `physionet-data.mimiciv_derived.inflammation`
where specimen_id is not null and hadm_id is not null

