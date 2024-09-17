create table `saved.e` as
select *
from `physionet-data.mimiciv_derived.enzyme`
where specimen_id is not null and hadm_id is not null
