CREATE OR REPLACE TABLE `saved.gpt_short_input` AS 

select subject_id, hadm_id, diagnoses, 
("Blood report: \n" || short_blood_report || coalesce("\nBlood gas report: \n" || blood_gas_report, "") || "\nImaging report: \n" || imaging_report || coalesce("\nMicrobiology tests: \n" || infection_report, "") || coalesce("\nVitalsigns data from ICU: \n" || vitalsign_report, "")) as GPT_input
from `saved.aggregation_short`