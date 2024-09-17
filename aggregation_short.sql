CREATE OR REPLACE TABLE `saved.aggregation_short` AS 

select i.subject_id, i.hadm_id, i.imaging_report, i.diagnoses, b.short_blood_report, 
bg.blood_gas_report, inf.infection_report, v.vitalsign_report, icp.icp_report
from `saved.agg_diag_and_notes` i
inner join `saved.blood_short` b
on i.hadm_id = b.admission_id
inner join `saved.blood_gas_aggregation_only` bg
on i.hadm_id = bg.hadm_id
-- left join because it could be that there's no infection and the infection data was filtered for no nulls
left join `saved.agg_infection` inf
on i.hadm_id = inf.hadm_id
-- left join because it could be that the patient was not in the ICU and hence has no measurements in vitalsign
left join `saved.vitalsign` v
on i.hadm_id = v.hadm_id
left join `saved.icp` icp
on i.hadm_id = icp.hadm_id