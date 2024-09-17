create table `saved.bd` as
select *
from `physionet-data.mimiciv_derived.blood_differential`
where specimen_id is not null and hadm_id is not null