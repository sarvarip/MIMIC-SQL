create or replace table `saved.blood_gas_aggregation_only` as

with df as 
(select 
--maybe add others now that it's not mixed with other dfs
--indeed, let's add hematocrit, hemoglobin and glucose for continuous monitoring
subject_id, hadm_id, so2, po2, pco2, fio2, aado2, ph, baseexcess, bicarbonate,
totalco2, carboxyhemoglobin, methemoglobin, lactate, peep, charttime,
hematocrit, hemoglobin, glucose,
case 
  when specimen = 'ART.' then 'arterial blood' 
  when specimen = 'VEN.' then 'venous blood' 
  when specimen = 'CENTRAL VENOUS.' then 'central venous blood' end as loc
from `saved.bg`
),

df_joined as
(
  select i.*, a.admittime, DATETIME_DIFF(i.charttime, a.admittime, HOUR) as hour_offset
  from df i
  --inner join to only consider patients admitted to hospital
  inner join `physionet-data.mimiciv_hosp.admissions` a
  on i.hadm_id = a.hadm_id
),

concatenated as 
(
select subject_id, hadm_id, hour_offset,
coalesce(CAST(hour_offset AS STRING) || ' hours after admission the blood gas results from ' || loc || ' are: ', '') ||
coalesce('ph: '                                     || CAST(ph AS STRING), '')                ||
coalesce(', po2: '                                  || CAST(po2 AS STRING), '')               ||
coalesce(', pco2: '                                 || CAST(pco2 AS STRING), '')              ||
coalesce(', fio2: '                                 || CAST(fio2 AS STRING), '')              ||
coalesce(', aado2: '                                || CAST(aado2 AS STRING), '')             ||
coalesce(', so2: '                                  || CAST(so2 AS STRING), '')               ||
coalesce(', base excess: '                          || CAST(baseexcess AS STRING), '')        ||
coalesce(', bicarbonate: '                          || CAST(bicarbonate AS STRING), '')       ||
coalesce(', total co2: '                            || CAST(totalco2 AS STRING), '')          ||
coalesce(', carboxyhemoglobin: '                    || CAST(carboxyhemoglobin AS STRING), '') ||
coalesce(', methemoglobin: '                        || CAST(methemoglobin AS STRING), '')     ||
coalesce(', glucose: '                              || CAST(glucose AS STRING), '')           ||
coalesce(', hematocrit: '                           || CAST(hematocrit AS STRING), '')        ||
coalesce(', hemoglobin (g/dl): '                    || CAST(hemoglobin AS STRING), '')        ||
coalesce(', peep: '                                 || CAST(peep AS STRING), '')              ||
coalesce(', lactate: '                              || CAST(lactate AS STRING), '') as blood_gas_report
from df_joined
)

select subject_id, hadm_id, string_agg(blood_gas_report, '\n' order by hour_offset) AS blood_gas_report
from concatenated
group by subject_id, hadm_id