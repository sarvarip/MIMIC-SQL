create table `saved.chem` as
select *
from `physionet-data.mimiciv_derived.chemistry`
where specimen_id is not null and hadm_id is not null