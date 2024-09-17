create table `saved.bg` as
select *
from `saved.blood_gas`
where specimen is not null and specimen != 'MIX.'