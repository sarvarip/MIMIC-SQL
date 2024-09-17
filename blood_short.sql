#standardsql

CREATE OR REPLACE TABLE `saved.blood_short` AS 

with df as (
select i.* except(subject_id, hadm_id, charttime, specimen_id), 
c.* except(subject_id, hadm_id, charttime, specimen_id),
chem.* except(subject_id, hadm_id, charttime, specimen_id, bicarbonate),
coag.* except(subject_id, hadm_id, charttime, specimen_id),
bc.* except(subject_id, hadm_id, charttime, specimen_id, wbc),
e.* except(subject_id, hadm_id, charttime, specimen_id),
crp.crp,

--this is important, because outer join will produce nans in variables that will later be used for groupby.. one of these variables however, will be 
--not nan since it is (hopefully) not nan in the original dataset.. so it's just a matter of finding and using the not nan one!
COALESCE(i.hadm_id, bc.hadm_id, c.hadm_id, chem.hadm_id, coag.hadm_id, e.hadm_id, crp.hadm_id) AS admission_id,

COALESCE(i.subject_id, bc.subject_id, c.subject_id, chem.subject_id, coag.subject_id, e.subject_id, crp.subject_id) AS patient_id,

COALESCE(i.charttime, bc.charttime, c.charttime, chem.charttime, coag.charttime, e.charttime, crp.charttime) AS measurement_time,

COALESCE(i.specimen_id, bc.specimen_id, c.specimen_id, chem.specimen_id, coag.specimen_id, e.specimen_id, crp.specimen_id) AS specimen_id_final,

--wbc is in blood differential and blood count (although it's the same sample actually)
--bicarbonate is excluded from chemistry as we don't know whether it's venous or arterial
--ck_mb is both in cardiac and enzyme
COALESCE(c.ck_mb, e.ck_mb) AS ck_mb_final

--https://stackoverflow.com/questions/16167902/multiple-full-outer-join-on-multiple-tables
--note this is merging on specimen and not hadm_id, so we have to use union all!
from (
  select distinct specimen_id from(
      select distinct specimen_id from `saved.bd`
      union all
      select distinct specimen_id from `saved.bc`
      union all
      select distinct specimen_id from `saved.c`
      union all
      select distinct specimen_id from `saved.chem`
      union all
      select distinct specimen_id from `saved.coag`
      union all
      select distinct specimen_id from `saved.e`
      union all
      select distinct specimen_id from `saved.crp`
  )
) as X
left join `saved.bd` i on i.specimen_id = X.specimen_id
left join `saved.bc` bc on bc.specimen_id = X.specimen_id
left join `saved.c` c on c.specimen_id = X.specimen_id
left join `saved.chem` chem on chem.specimen_id = X.specimen_id
left join `saved.coag` coag on coag.specimen_id = X.specimen_id
left join `saved.e` e on e.specimen_id = X.specimen_id
left join `saved.crp` crp on crp.specimen_id = X.specimen_id
)


select patient_id, admission_id, 
'The patient stayed in the hospital and had the following statistics on the blood test measurements during the stay: ' ||
coalesce('min wbc: '                            || CAST(min(wbc) AS STRING), '')                 ||
coalesce(', min absolute basophils: '           || CAST(min(basophils_abs) AS STRING), '')       ||
coalesce(', min absolute eosinophils: '         || CAST(min(eosinophils_abs) AS STRING), '')     ||
coalesce(', min absolute monocytes: '           || CAST(min(monocytes_abs) AS STRING), '')       ||
coalesce(', min absolute neutrophils: '         || CAST(min(neutrophils_abs) AS STRING), '')     ||
coalesce(', min basophils: '                    || CAST(min(basophils) AS STRING), '')           ||
coalesce(', min eosinophils: '                  || CAST(min(eosinophils) AS STRING), '')         ||
coalesce(', min lymphocytes: '                  || CAST(min(lymphocytes) AS STRING), '')         ||
coalesce(', min monocytes: '                    || CAST(min(monocytes) AS STRING), '')           ||
coalesce(', min neutrophils: '                  || CAST(min(neutrophils) AS STRING), '')         ||
coalesce(', min atypical lymphocytes: '         || CAST(min(atypical_lymphocytes) AS STRING), '')||
coalesce(', min bands (%): '                    || CAST(min(bands) AS STRING), '')               ||
coalesce(', min immature granulocytes: '        || CAST(min(immature_granulocytes) AS STRING), '') ||
coalesce(', min metamyelocytes: '               || CAST(min(metamyelocytes) AS STRING), '')      ||
coalesce(', min nrbc: '                         || CAST(min(nrbc) AS STRING), '')                ||
coalesce(', min troponin T: '                   || CAST(min(troponin_t) AS STRING), '') ||
coalesce(', min ck_mb: '                        || CAST(min(ck_mb_final) AS STRING), '')         ||
coalesce(', min ntprobnp: '                     || CAST(min(ntprobnp) AS STRING), '')            ||
coalesce(', min albumin: '                      || CAST(min(albumin) AS STRING), '')             ||
coalesce(', min globulin: '                     || CAST(min(globulin) AS STRING), '')            ||
coalesce(', min total protein: '                || CAST(min(total_protein) AS STRING), '')       ||
coalesce(', min aniongap: '                     || CAST(min(aniongap) AS STRING), '')            ||
coalesce(', min bun: '                          || CAST(min(bun) AS STRING), '')                 ||
coalesce(', min calcium: '                      || CAST(min(calcium) AS STRING), '')             ||
coalesce(', min chloride: '                     || CAST(min(chloride) AS STRING), '')            ||
coalesce(', min creatinine: '                   || CAST(min(creatinine) AS STRING), '')          ||
coalesce(', min glucose: '                      || CAST(min(glucose) AS STRING), '')             ||
coalesce(', min sodium: '                       || CAST(min(sodium) AS STRING), '')              ||
coalesce(', min potassium: '                    || CAST(min(potassium) AS STRING), '')           ||
coalesce(', min d_dimer: '                      || CAST(min(d_dimer) AS STRING), '')             ||
coalesce(', min fibrinogen: '                   || CAST(min(fibrinogen) AS STRING), '')          ||
coalesce(', min inr: '                          || CAST(min(inr) AS STRING), '')                 ||
coalesce(', min pt: '                           || CAST(min(pt) AS STRING), '')                  ||
coalesce(', min ptt: '                          || CAST(min(ptt) AS STRING), '')                 ||
coalesce(', min hematocrit: '                   || CAST(min(hematocrit) AS STRING), '')          ||
coalesce(', min hemoglobin (g/dl): '            || CAST(min(hemoglobin) AS STRING), '')          ||
coalesce(', min mch: '                          || CAST(min(mch) AS STRING), '')                 ||
coalesce(', min mchc: '                         || CAST(min(mchc) AS STRING), '')                ||
coalesce(', min mcv: '                          || CAST(min(mcv) AS STRING), '')                 ||
coalesce(', min platelet: '                     || CAST(min(platelet) AS STRING), '')            ||
coalesce(', min rbc: '                          || CAST(min(rbc) AS STRING), '')                 ||
coalesce(', min rdw: '                          || CAST(min(rdw) AS STRING), '')                 ||
coalesce(', min rdw standard deviation: '       || CAST(min(rdwsd) AS STRING), '')               ||
coalesce(', min alt: '                          || CAST(min(alt) AS STRING), '')                 ||
coalesce(', min alp: '                          || CAST(min(alp) AS STRING), '')                 ||
coalesce(', min ast: '                          || CAST(min(ast) AS STRING), '')                 ||
coalesce(', min total bilirubin: '              || CAST(min(bilirubin_total) AS STRING), '')     ||
coalesce(', min direct bilirubin: '             || CAST(min(bilirubin_direct) AS STRING), '')    ||
coalesce(', min indirect bilirubin: '           || CAST(min(bilirubin_indirect) AS STRING), '')  ||
coalesce(', min ck_cpk: '                       || CAST(min(ck_cpk) AS STRING), '')              ||
coalesce(', min ggt: '                          || CAST(min(ggt) AS STRING), '')                 ||

coalesce(', max wbc: '                          || CAST(max(wbc) AS STRING), '')                 ||
coalesce(', max absolute basophils: '           || CAST(max(basophils_abs) AS STRING), '')       ||
coalesce(', max absolute eosinophils: '         || CAST(max(eosinophils_abs) AS STRING), '')     ||
coalesce(', max absolute monocytes: '           || CAST(max(monocytes_abs) AS STRING), '')       ||
coalesce(', max absolute neutrophils: '         || CAST(max(neutrophils_abs) AS STRING), '')     ||
coalesce(', max basophils: '                    || CAST(max(basophils) AS STRING), '')           ||
coalesce(', max eosinophils: '                  || CAST(max(eosinophils) AS STRING), '')         ||
coalesce(', max lymphocytes: '                  || CAST(max(lymphocytes) AS STRING), '')         ||
coalesce(', max monocytes: '                    || CAST(max(monocytes) AS STRING), '')           ||
coalesce(', max neutrophils: '                  || CAST(max(neutrophils) AS STRING), '')         ||
coalesce(', max atypical lymphocytes: '         || CAST(max(atypical_lymphocytes) AS STRING), '')||
coalesce(', max bands (%): '                    || CAST(max(bands) AS STRING), '')               ||
coalesce(', max immature granulocytes: '        || CAST(max(immature_granulocytes) AS STRING), '') ||
coalesce(', max metamyelocytes: '               || CAST(max(metamyelocytes) AS STRING), '')      ||
coalesce(', max nrbc: '                         || CAST(max(nrbc) AS STRING), '')                ||
coalesce(', max troponin T: '                   || CAST(max(troponin_t) AS STRING), '') ||
coalesce(', max ck_mb: '                        || CAST(max(ck_mb_final) AS STRING), '')         ||
coalesce(', maxntprobnp: '                      || CAST(max(ntprobnp) AS STRING), '')            ||
coalesce(', max albumin: '                      || CAST(max(albumin) AS STRING), '')             ||
coalesce(', max globulin: '                     || CAST(max(globulin) AS STRING), '')            ||
coalesce(', max total protein: '                || CAST(max(total_protein) AS STRING), '')       ||
coalesce(', max aniongap: '                     || CAST(max(aniongap) AS STRING), '')            ||
coalesce(', max bun: '                          || CAST(max(bun) AS STRING), '')                 ||
coalesce(', max calcium: '                      || CAST(max(calcium) AS STRING), '')             ||
coalesce(', max chloride: '                     || CAST(max(chloride) AS STRING), '')            ||
coalesce(', max creatinine: '                   || CAST(max(creatinine) AS STRING), '')          ||
coalesce(', max glucose: '                      || CAST(max(glucose) AS STRING), '')             ||
coalesce(', max sodium: '                       || CAST(max(sodium) AS STRING), '')              ||
coalesce(', max potassium: '                    || CAST(max(potassium) AS STRING), '')           ||
coalesce(', max d_dimer: '                      || CAST(max(d_dimer) AS STRING), '')             ||
coalesce(', max fibrinogen: '                   || CAST(max(fibrinogen) AS STRING), '')          ||
coalesce(', max inr: '                          || CAST(max(inr) AS STRING), '')                 ||
coalesce(', max pt: '                           || CAST(max(pt) AS STRING), '')                  ||
coalesce(', max ptt: '                          || CAST(max(ptt) AS STRING), '')                 ||
coalesce(', max hematocrit: '                   || CAST(max(hematocrit) AS STRING), '')          ||
coalesce(', max hemoglobin (g/dl): '            || CAST(max(hemoglobin) AS STRING), '')          ||
coalesce(', max mch: '                          || CAST(max(mch) AS STRING), '')                 ||
coalesce(', max mchc: '                         || CAST(max(mchc) AS STRING), '')                ||
coalesce(', max mcv: '                          || CAST(max(mcv) AS STRING), '')                 ||
coalesce(', max platelet: '                     || CAST(max(platelet) AS STRING), '')            ||
coalesce(', max rbc: '                          || CAST(max(rbc) AS STRING), '')                 ||
coalesce(', max rdw: '                          || CAST(max(rdw) AS STRING), '')                 ||
coalesce(', max rdw standard deviation: '       || CAST(max(rdwsd) AS STRING), '')               ||
coalesce(', max alt: '                          || CAST(max(alt) AS STRING), '')                 ||
coalesce(', max alp: '                          || CAST(max(alp) AS STRING), '')                 ||
coalesce(', max ast: '                          || CAST(max(ast) AS STRING), '')                 ||
coalesce(', max total bilirubin: '              || CAST(max(bilirubin_total) AS STRING), '')     ||
coalesce(', max direct bilirubin: '             || CAST(max(bilirubin_direct) AS STRING), '')    ||
coalesce(', max indirect bilirubin: '           || CAST(max(bilirubin_indirect) AS STRING), '')  ||
coalesce(', max ck_cpk: '                       || CAST(max(ck_cpk) AS STRING), '')              ||
coalesce(', max ggt: '                          || CAST(max(ggt) AS STRING), '')                 ||

coalesce(', avg wbc: '                          || CAST(ROUND(avg(wbc),2) AS STRING), '')                 ||
coalesce(', avg absolute basophils: '           || CAST(ROUND(avg(basophils_abs),2) AS STRING), '')       ||
coalesce(', avg absolute eosinophils: '         || CAST(ROUND(avg(eosinophils_abs),2) AS STRING), '')     ||
coalesce(', avg absolute monocytes: '           || CAST(ROUND(avg(monocytes_abs),2) AS STRING), '')       ||
coalesce(', avg absolute neutrophils: '         || CAST(ROUND(avg(neutrophils_abs),2) AS STRING), '')     ||
coalesce(', avg basophils: '                    || CAST(ROUND(avg(basophils),2) AS STRING), '')           ||
coalesce(', avg eosinophils: '                  || CAST(ROUND(avg(eosinophils),2) AS STRING), '')         ||
coalesce(', avg lymphocytes: '                  || CAST(ROUND(avg(lymphocytes),2) AS STRING), '')         ||
coalesce(', avg monocytes: '                    || CAST(ROUND(avg(monocytes),2) AS STRING), '')           ||
coalesce(', avg neutrophils: '                  || CAST(ROUND(avg(neutrophils),2) AS STRING), '')         ||
coalesce(', avg atypical lymphocytes: '         || CAST(ROUND(avg(atypical_lymphocytes),2) AS STRING), '')||
coalesce(', avg bands (%): '                    || CAST(ROUND(avg(bands),2) AS STRING), '')               ||
coalesce(', avg immature granulocytes: '        || CAST(ROUND(avg(immature_granulocytes),2) AS STRING), '') ||
coalesce(', avg metamyelocytes: '               || CAST(ROUND(avg(metamyelocytes),2) AS STRING), '')      ||
coalesce(', avg nrbc: '                         || CAST(ROUND(avg(nrbc),2) AS STRING), '')                ||
coalesce(', avg troponin T: '                   || CAST(ROUND(avg(troponin_t),2) AS STRING), '') ||
coalesce(', avg ck_mb: '                        || CAST(ROUND(avg(ck_mb_final),2) AS STRING), '')         ||
coalesce(', avg ntprobnp: '                     || CAST(ROUND(avg(ntprobnp),2) AS STRING), '')            ||
coalesce(', avg albumin: '                      || CAST(ROUND(avg(albumin),2) AS STRING), '')             ||
coalesce(', avg globulin: '                     || CAST(ROUND(avg(globulin),2) AS STRING), '')            ||
coalesce(', avg total protein: '                || CAST(ROUND(avg(total_protein),2) AS STRING), '')       ||
coalesce(', avg aniongap: '                     || CAST(ROUND(avg(aniongap),2) AS STRING), '')            ||
coalesce(', avg bun: '                          || CAST(ROUND(avg(bun),2) AS STRING), '')                 ||
coalesce(', avg calcium: '                      || CAST(ROUND(avg(calcium),2) AS STRING), '')             ||
coalesce(', avg chloride: '                     || CAST(ROUND(avg(chloride),2) AS STRING), '')            ||
coalesce(', avg creatinine: '                   || CAST(ROUND(avg(creatinine),2) AS STRING), '')          ||
coalesce(', avg glucose: '                      || CAST(ROUND(avg(glucose),2) AS STRING), '')             ||
coalesce(', avg sodium: '                       || CAST(ROUND(avg(sodium),2) AS STRING), '')              ||
coalesce(', avg potassium: '                    || CAST(ROUND(avg(potassium),2) AS STRING), '')           ||
coalesce(', avg d_dimer: '                      || CAST(ROUND(avg(d_dimer),2) AS STRING), '')             ||
coalesce(', avg fibrinogen: '                   || CAST(ROUND(avg(fibrinogen),2) AS STRING), '')          ||
coalesce(', avg inr: '                          || CAST(ROUND(avg(inr),2) AS STRING), '')                 ||
coalesce(', avg pt: '                           || CAST(ROUND(avg(pt),2) AS STRING), '')                  ||
coalesce(', avg ptt: '                          || CAST(ROUND(avg(ptt),2) AS STRING), '')                 ||
coalesce(', avg hematocrit: '                   || CAST(ROUND(avg(hematocrit),2) AS STRING), '')          ||
coalesce(', avg hemoglobin (g/dl): '            || CAST(ROUND(avg(hemoglobin),2) AS STRING), '')          ||
coalesce(', avg mch: '                          || CAST(ROUND(avg(mch),2) AS STRING), '')                 ||
coalesce(', avg mchc: '                         || CAST(ROUND(avg(mchc),2) AS STRING), '')                ||
coalesce(', avg mcv: '                          || CAST(ROUND(avg(mcv),2) AS STRING), '')                 ||
coalesce(', avg platelet: '                     || CAST(ROUND(avg(platelet),2) AS STRING), '')            ||
coalesce(', avg rbc: '                          || CAST(ROUND(avg(rbc),2) AS STRING), '')                 ||
coalesce(', avg rdw: '                          || CAST(ROUND(avg(rdw),2) AS STRING), '')                 ||
coalesce(', avg rdw standard deviation: '       || CAST(ROUND(avg(rdwsd),2) AS STRING), '')               ||
coalesce(', avg alt: '                          || CAST(ROUND(avg(alt),2) AS STRING), '')                 ||
coalesce(', avg alp: '                          || CAST(ROUND(avg(alp),2) AS STRING), '')                 ||
coalesce(', avg ast: '                          || CAST(ROUND(avg(ast),2) AS STRING), '')                 ||
coalesce(', avg total bilirubin: '              || CAST(ROUND(avg(bilirubin_total),2) AS STRING), '')     ||
coalesce(', avg direct bilirubin: '             || CAST(ROUND(avg(bilirubin_direct),2) AS STRING), '')    ||
coalesce(', avg indirect bilirubin: '           || CAST(ROUND(avg(bilirubin_indirect),2) AS STRING), '')  ||
coalesce(', avg ck_cpk: '                       || CAST(ROUND(avg(ck_cpk),2) AS STRING), '')              ||
coalesce(', avg ggt: '                          || CAST(ROUND(avg(ggt),2) AS STRING), '') as short_blood_report

from df
group by patient_id, admission_id