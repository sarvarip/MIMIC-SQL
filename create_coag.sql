create table `saved.coag` as
select *
from `physionet-data.mimiciv_derived.coagulation`
where specimen_id is not null and hadm_id is not null