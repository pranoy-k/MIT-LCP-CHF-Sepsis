
---------------------------------------------------------------------------------------------------------

-- Project Title: How much fluid should we give to septic patients with heart failure?

---------------------------------------------------------------------------------------------------------

-- This project will have three aims, each with a separate corresponding analysis:

-- Evaluate differences in fluid resuscitation in the ICU among septic patients with and without CHF.
-- Assess differences in fluid resuscitation among septic patients in the ICU with different types of CHF.
-- Examine differences in outcomes among patients with different types of CHF according to amount of fluid received.

---------------------------------------------------------------------------------------------------------

-- Background:

-- With respect to fluid resuscitation the ideal amount of fluid to administer is unknown, and recent research
-- has demonstrated that excessive fluid resuscitation is likely to be harmful.

-- Patients with congestive heart failure (CHF) are more likely to suffer the negative impact of excess fluid 
-- administration. Fluid accumulation in these patients causes exacerbation of CHF - pulmonary edema leading
-- to respiratory failure and poor pump function leading to cardiogenic shock. 

-- The available studies suggest that there is clinical confusion and scientific uncertainty surrounding how 
-- much fluid to use when resuscitating sepsis/CHF patients. There are multiple phenotypes of heart failure
-- (HFrEF and HFpEF) which may respond differently to fluid resuscitation levels.

-- sCHF or HFrEF -- systolic dysfunction or heart failure with reduced ejection fraction  
-- dCHF or HFpEF -- diastolic dysfunction or heart failure with preserved ejection fraction

---------------------------------------------------------------------------------------------------------

-- POSTGRES CODE pgadmin v3------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------
 -- Create admission index, to track the first encounter of admission CreateView_Admission_index.sql --
---------------------------------------------------------------------------------------------------------

set search_path to public, mimiciii;
DROP table if exists mimiciii_admissions_index cascade; 
CREATE 
table mimiciii_admissions_index as
SELECT 
patients.subject_id,
admissions.hadm_id,
icustays.icustay_id,
DENSE_RANK() over(PARTITION BY patients.subject_id, admissions.hadm_id ORDER BY icustays.icustay_id  ) AS ind
FROM PATIENTS patients
LEFT JOIN ADMISSIONS admissions
ON patients.subject_id = admissions.subject_id
LEFT JOIN ICUSTAYS icustays
ON patients.subject_id = icustays.subject_id and admissions.hadm_id = icustays.hadm_id
ORDER BY patients.subject_id, admissions.hadm_id;



--SELECT *
--FROM mimiciii_admissions_index
--where IND = 1;

--'''MIMIC admission (first encounter)'''
-- SELECT *
-- FROM mimiciii_admissions_index
-- where IND = 1
-- 
-- SELECT distinct subject_id
-- FROM mimiciii_admissions_index
-- where IND = 1

-- Derive patient age - cohort_patient_demographics.sql
Drop table if exists cohort_patient_demographics;
SELECT cohort.subject_id, cohort.hadm_id, cohort.admittime, dob, age(admittime,dob) as age_db, extract(year FROM age(admittime,dob) ) AS AGE,
     (Case when extract(year FROM age(admittime,dob) ) < 18 Then 0 else  1
        END ) as age_flag, dod, dod_hosp, dod_ssn, expire_flag
    INTO cohort_patient_demographics
    FROM admissions cohort
    LEFT JOIN patients patient 
    on cohort.subject_id = patient.subject_id; 

select count(*) FROM 
mimiciii_admissions_index cohort
left join cohort_patient_demographics patient
on cohort.subject_id = patient.subject_id
and cohort.hadm_id = patient.hadm_id
where age_flag = 1; -- 54401*/
    
-- -- EXCLUSION AND INCLUSION CRITERIA --
-- 
-- --  Age filter on chf:
-- select count(*) FROM mimiciii_admissions_index cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 -- 14309
-- 
-- select distinct patient.subject_id FROM mimiciii_admissions_index cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 -- 10353 



---------------------------------------------------------------------------------------------------------
 --  Calculate angus score for sepsis- angus_sepsis.sql --
---------------------------------------------------------------------------------------------------------


-- ------------------------------------------------------------------
-- Title: Calculate angus score for sepsis
-- Notes: this query does not specify a schema. To run it on your local
-- MIMIC schema, run the following command:
set search_path to public, mimiciii;
-- Where "public, mimiciii" is the name of your schema, and may be different.
-- ------------------------------------------------------------------
-- ICD-9 codes for Angus criteria of sepsis
-- Angus et al, 2001. Epidemiology of severe sepsis in the United States
-- http://www.ncbi.nlm.nih.gov/pubmed/11445675
-- Case selection and definitions
-- To identify cases with severe sepsis, we selected all acute care hospitalizations with ICD-9-CM codes for both:
-- (a) a bacterial or fungal infectious process AND
-- (b) a diagnosis of acute organ dysfunction (Appendix 2).
-- THIS CODE SHOULD FOLLOW CreateView_Admission_index.sql
----------------------------------------------------------------------

--Permission denied for relations of angus_sepsis, so included "mimiciii_"
DROP table IF EXISTS mimiciii_angus_sepsis CASCADE;
CREATE table mimiciii_angus_sepsis as
-- ICD-9 codes for infection - as sourced from Appendix 1 of above paper
WITH infection_group AS
(
    SELECT subject_id, hadm_id,
    CASE
        WHEN substring(icd9_code,1,3) IN ('001','002','003','004','005','008',
               '009','010','011','012','013','014','015','016','017','018',
               '020','021','022','023','024','025','026','027','030','031',
               '032','033','034','035','036','037','038','039','040','041',
               '090','091','092','093','094','095','096','097','098','100',
               '101','102','103','104','110','111','112','114','115','116',
               '117','118','320','322','324','325','420','421','451','461',
               '462','463','464','465','481','482','485','486','494','510',
               '513','540','541','542','566','567','590','597','601','614',
               '615','616','681','682','683','686','730') THEN 1
        WHEN substring(icd9_code,1,4) IN ('5695','5720','5721','5750','5990','7110',
                '7907','9966','9985','9993') THEN 1
        WHEN substring(icd9_code,1,5) IN ('49121','56201','56203','56211','56213',
                '56983') THEN 1
        ELSE 0 END AS infection
    FROM diagnoses_icd
),
-- ICD-9 codes for organ dysfunction - as sourced from Appendix 2 of above paper
organ_diag_group as
(
    SELECT subject_id, hadm_id,
        CASE
        -- Acute Organ Dysfunction Diagnosis Codes
        WHEN substring(icd9_code,1,3) IN ('458','293','570','584') THEN 1
        WHEN substring(icd9_code,1,4) IN ('7855','3483','3481',
                '2874','2875','2869','2866','5734')  THEN 1
        ELSE 0 END AS organ_dysfunction,
        -- Explicit diagnosis of severe sepsis or septic shock
        CASE
        WHEN substring(icd9_code,1,5) IN ('99592','78552')  THEN 1
        ELSE 0 END AS explicit_sepsis
    FROM diagnoses_icd
),
-- Mechanical ventilation
organ_proc_group as
(
    SELECT subject_id, hadm_id,
        CASE
        WHEN substring(icd9_code,1,4) IN ('9670','9671','9672') THEN 1
        ELSE 0 END AS mech_vent
    FROM procedures_icd
),
-- Aggregate above views together
aggregate as
(
    SELECT subject_id, hadm_id,
        CASE
            WHEN hadm_id in
                    (SELECT DISTINCT hadm_id
                    FROM infection_group
                    WHERE infection = 1)
                THEN 1
            ELSE 0 END AS infection,
        CASE
            WHEN hadm_id in
                    (SELECT DISTINCT hadm_id
                    FROM organ_diag_group
                    WHERE explicit_sepsis = 1)
                THEN 1
            ELSE 0 END AS explicit_sepsis,
        CASE
            WHEN hadm_id in
                    (SELECT DISTINCT hadm_id
                    FROM organ_diag_group
                    WHERE organ_dysfunction = 1)
                THEN 1
            ELSE 0 END AS organ_dysfunction,
        CASE
        WHEN hadm_id in
                (SELECT DISTINCT hadm_id
                FROM organ_proc_group
                WHERE mech_vent = 1)
            THEN 1
        ELSE 0 END AS mech_vent
    FROM admissions
)
-- Output component flags (explicit sepsis, organ dysfunction) and final flag (angus)
SELECT subject_id, hadm_id, infection,
   explicit_sepsis, organ_dysfunction, mech_vent,
CASE
    WHEN explicit_sepsis = 1 THEN 1
    WHEN infection = 1 AND organ_dysfunction = 1 THEN 1
    WHEN infection = 1 AND mech_vent = 1 THEN 1
    ELSE 0 END
AS angus
FROM aggregate;

select subject_id, count(*) from mimiciii_angus_sepsis where angus = 1 group by subject_id;

select count(distinct subject_id) from mimiciii_angus_sepsis where angus = 1; -- 12,636

select count(*) from mimiciii_angus_sepsis where angus = 1; -- 15,254 

---------------------------------------------------------------------------------------------------------
 --  Create CHD cohort: CHD_cohort.sql --
---------------------------------------------------------------------------------------------------------


-- ------------------------------------------------------------------
-- Title: Create CHD cohort
-- Notes: this query does not specify a schema. To run it on your local
-- MIMIC schema, run the following command:
SET SEARCH_PATH TO mimiciii;
-- Where "mimiciii" is the name of your schema, and may be different.
-- ------------------------------------------------------------------
-- *Changed mimic.angus_sepsis_tbt --> mimiciii_angus_sepsis_tbt
-- ------------------------------------------------------------------
-- Should have already run the followig scripts prior to this
-- a) CreateView_Admission_index.sql
-- b) angus_sepsis.sql
-- ------------------------------------------------------------------
-- ICD-9 codes for CHF based on:  http://www.icd9data.com/2015/Volume1/390-459/420-429/428/default.htm

--    Congestive heart failure unspecified ='4280'
--        Left heart failure (Pulmonary edema, acute) = '4281'
--        Systolic heart failure= '42820'
--        Acute systolic heart failure = '42821'
--        Chronic systolic heart failure = '42822'
--        Acute on chronic systolic heart failure= '42823'
--        Diastolic heart failure= '42830'
--        Acute diastolic heart failure = '42831'
--        Chronic diastolic heart failure = '42832'
--        Acute on chronic diastolic heart failure = '42833'
--        Combined systolic and diastolic heart failure = '42840'
--        Acute combined systolic and diastolic heart failure = '42841'
--        Chronic combined systolic and diastolic heart failure = '42842'
--        Acute on chronic combined systolic and diastolic heart failure = '42843'
--        Heart failure, unspecified = '4289'

-- DRG

-- icd9_code = '39891' 

-- when icd9_code = '40201' then 1
-- when icd9_code = '40211' then 1
-- when icd9_code = '40291' then 1
-- end as HTNWCHF /* Hypertensive heart disease with heart failure */
-- 
-- 
-- when icd9_code = '40401' then 1
-- when icd9_code = '40411' then 1
-- when icd9_code = '40491' then 1
-- end as HHRWCHF /* Hypertensive heart and renal disease with heart failure */
-- -------------------------------------------------------------------

DROP table IF EXISTS mimiciii_angus_sepsis_tbt; 
SELECT * 
INTO mimiciii_angus_sepsis_tbt
FROM mimiciii_angus_sepsis;
ALTER TABLE mimiciii_angus_sepsis_tbt ADD COLUMN CHD INTEGER;
UPDATE mimiciii_angus_sepsis_tbt SET CHD = 0;

UPDATE mimiciii_angus_sepsis_tbt SET CHD = 1
 from  diagnoses_icd diagnoses_idc
    where (diagnoses_idc.icd9_code = '4280'
        or diagnoses_idc.icd9_code = '4281'
    or diagnoses_idc.icd9_code = '4289'
        or diagnoses_idc.icd9_code = '42820'
        or diagnoses_idc.icd9_code = '42821'
        or diagnoses_idc.icd9_code = '42822'
        or diagnoses_idc.icd9_code = '42823'
        or diagnoses_idc.icd9_code = '42830'
        or diagnoses_idc.icd9_code = '42831'
        or diagnoses_idc.icd9_code = '42832'
        or diagnoses_idc.icd9_code = '42833'
        or diagnoses_idc.icd9_code = '42840' 
        or diagnoses_idc.icd9_code = '42841'
        or diagnoses_idc.icd9_code = '42843'
    or diagnoses_idc.icd9_code = '42842'
    or diagnoses_idc.icd9_code = '39891' -- DRG
    or diagnoses_idc.icd9_code = '40201'
    or diagnoses_idc.icd9_code = '40211'
    or diagnoses_idc.icd9_code = '40291'
    or diagnoses_idc.icd9_code = '40401'
    or diagnoses_idc.icd9_code = '40411'
    or diagnoses_idc.icd9_code = '40491'
        ) and 
 mimiciii_angus_sepsis_tbt.hadm_id = diagnoses_idc.hadm_id
AND mimiciii_angus_sepsis_tbt.subject_id = diagnoses_idc.subject_id;


-- Checking --
-- 
-- select * 
-- from 
-- (
-- select  distinct icd9_code
-- from mimiciii_angus_sepsis_tbt sepsis
-- left join diagnoses_icd diagnoses_icd
-- on sepsis.subject_id = diagnoses_icd.subject_id
-- and sepsis.hadm_id = diagnoses_icd.hadm_id 
-- where CHD= 0) as ICD 
-- where ICD.icd9_code = '4280'
-- or ICD.icd9_code = '4281'
-- or ICD.icd9_code = '4289'
-- or ICD.icd9_code = '42820'
-- or ICD.icd9_code = '42821'
-- or ICD.icd9_code = '42822'
-- or ICD.icd9_code = '42823'
-- or ICD.icd9_code = '42830'
-- or ICD.icd9_code = '42831'
-- or ICD.icd9_code = '42832'
-- or ICD.icd9_code = '42833'
-- or ICD.icd9_code = '42840' 
-- or ICD.icd9_code = '42841'
-- or ICD.icd9_code = '42842'
-- or ICD.icd9_code = '42843';
--
--
--select count(*) from mimiciii_angus_sepsis_tbt where chd = 1; --14040 
--
--select distinct subject_id from mimiciii_angus_sepsis_tbt where chd = 1; -- 10436

ALTER TABLE mimiciii_angus_sepsis_tbt ADD COLUMN CHD_CLASS VARCHAR(20);


UPDATE mimiciii_angus_sepsis_tbt SET CHD_CLASS = 'SYSTOLIC'
 from  diagnoses_icd diagnoses_idc
    where (
        diagnoses_idc.icd9_code = '42820'
        or diagnoses_idc.icd9_code = '42821'
        or diagnoses_idc.icd9_code = '42822'
        or diagnoses_idc.icd9_code = '42823'
       ) and 
 mimiciii_angus_sepsis_tbt.hadm_id = diagnoses_idc.hadm_id
AND mimiciii_angus_sepsis_tbt.subject_id = diagnoses_idc.subject_id;

UPDATE mimiciii_angus_sepsis_tbt SET CHD_CLASS = 'COMBINED'
 from  diagnoses_icd diagnoses_idc
    where (
        diagnoses_idc.icd9_code = '42840' 
        or diagnoses_idc.icd9_code = '42841'
        or diagnoses_idc.icd9_code = '42842'
        or diagnoses_idc.icd9_code = '42843'
       ) and 
 mimiciii_angus_sepsis_tbt.hadm_id = diagnoses_idc.hadm_id
AND mimiciii_angus_sepsis_tbt.subject_id = diagnoses_idc.subject_id;


UPDATE mimiciii_angus_sepsis_tbt SET CHD_CLASS = 'DIASTOLIC'
 from  diagnoses_icd diagnoses_idc
    where (
        diagnoses_idc.icd9_code = '42830'
        or diagnoses_idc.icd9_code = '42831'
        or diagnoses_idc.icd9_code = '42831'
        or diagnoses_idc.icd9_code = '42832'
        or diagnoses_idc.icd9_code = '42833'
       ) and 
 mimiciii_angus_sepsis_tbt.hadm_id = diagnoses_idc.hadm_id
AND mimiciii_angus_sepsis_tbt.subject_id = diagnoses_idc.subject_id;


UPDATE mimiciii_angus_sepsis_tbt SET CHD_CLASS = 'UNSPECIFIED'
 from  diagnoses_icd diagnoses_idc
    where (
        diagnoses_idc.icd9_code = '4280'
        or diagnoses_idc.icd9_code = '4281'
         or diagnoses_idc.icd9_code = '4289'
       ) and 
 mimiciii_angus_sepsis_tbt.hadm_id = diagnoses_idc.hadm_id
AND mimiciii_angus_sepsis_tbt.subject_id = diagnoses_idc.subject_id;

UPDATE mimiciii_angus_sepsis_tbt SET CHD_CLASS = 'DRG'
 from  diagnoses_icd diagnoses_idc
    where (
        diagnoses_idc.icd9_code = '39891'
        or diagnoses_idc.icd9_code = '40201'
        or diagnoses_idc.icd9_code = '40211'
        or diagnoses_idc.icd9_code = '40291'
    or diagnoses_idc.icd9_code = '40401'
    or diagnoses_idc.icd9_code = '40411'
    or diagnoses_idc.icd9_code = '40491'
    or diagnoses_idc.icd9_code = '40403'
    or diagnoses_idc.icd9_code = '40413'
    or diagnoses_idc.icd9_code = '40493'
       ) and 
 mimiciii_angus_sepsis_tbt.hadm_id = diagnoses_idc.hadm_id
AND mimiciii_angus_sepsis_tbt.subject_id = diagnoses_idc.subject_id;

-- 
-- select count(*) from mimiciii_angus_sepsis_tbt where CHD_CLASS = 'SYSTOLIC' -- 140
-- select distinct subject_id from mimiciii_angus_sepsis_tbt where CHD_CLASS = 'SYSTOLIC' -- 137
-- 
-- 
-- select * from mimiciii_angus_sepsis_tbt where CHD_CLASS = 'COMBINED' -- 50
-- select distinct subject_id from mimiciii_angus_sepsis_tbt where CHD_CLASS = 'COMBINED' -- 50
-- 
-- 
-- select * from mimiciii_angus_sepsis_tbt where CHD_CLASS = 'DIASTOLIC' -- 254
-- select distinct subject_id from mimiciii_angus_sepsis_tbt where CHD_CLASS = 'DIASTOLIC' -- 245 
-- 
-- 
-- select * from mimiciii_angus_sepsis_tbt where CHD_CLASS = 'UNSPECIFIED' -- 13001 
-- select distinct subject_id from mimiciii_angus_sepsis_tbt where CHD_CLASS = 'UNSPECIFIED' -- 9796 
-- 
-- 
-- select * from mimiciii_angus_sepsis_tbt where CHD_CLASS = 'DRG' -- 595 
-- select distinct subject_id from mimiciii_angus_sepsis_tbt where CHD_CLASS = 'DRG' -- 555 



-- -- CHF --
-- 
-- Create a view with time 2 discharge for CHF table
set search_path to public, mimiciii;
 create view chd_discharge as 
 (
     SELECT 
         admissionindex.subject_id, admissionindex.hadm_id, admissionindex.icustay_id, admissionindex.ind,
         admissions.admittime, admissions.dischtime,angus.chd,angus.CHD_CLASS,
         EXTRACT(day FROM admissions.dischtime - admissions.admittime)*24+EXTRACT(HOUR FROM admissions.dischtime - admissions.admittime)  as timeAdtoDischarge,
         EXTRACT(day FROM admissions.deathtime - admissions.admittime)*24+EXTRACT(HOUR FROM admissions.deathtime - admissions.admittime)  as timeAdtoDeath
         FROM mimiciii_angus_sepsis_tbt  angus
         LEFT JOIN mimiciii_admissions_index admissionindex
         ON admissionindex.hadm_id = angus.hadm_id and angus.subject_id = admissionindex.subject_id
         LEFT JOIN admissions admissions
         ON admissionindex.hadm_id = admissions.hadm_id and admissions.subject_id = admissionindex.subject_id
         WHERE ind = 1 
     ); 
-- 

-- -- INCLUSION AND EXCLUSION CRITERIA --
-- 
--select count(*) from mimiciii_angus_sepsis_tbt where chd = 1; -- 14040
-- 
-- 
-- -- Time filter on chf:
-- select count(*) from chd_discharge where chd = 1 and timeAdtoDischarge <= 24*60 -- 13951
-- select distinct subject_id from chd_discharge where chd = 1 and timeAdtoDischarge <= 24*60 -- 10375  
-- 
-- select count(*) from chd_discharge where chd = 1 and timeAdtoDischarge <= 48*60 -- 14035
-- select distinct subject_id from chd_discharge where chd = 1 and timeAdtoDischarge <= 48*60 -- 10432  
-- 
-- --  Age filter on chf:
-- select count(*) FROM chd_discharge cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and chd = 1 -- 14,025
-- 
-- select count(distinct patient.subject_id) FROM chd_discharge cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and chd = 1 -- 10,421 
-- 
-- -- Age and time filters on chf
-- select count(*) FROM chd_discharge cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (chd = 1) and timeAdtoDischarge <= 24*60 -- 13,940
-- 
-- select count(distinct patient.subject_id) FROM chd_discharge cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (chd = 1) and timeAdtoDischarge <= 24*60 -- 10,364 
-- 
-- select count(*) FROM chd_discharge cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (chd = 1) and timeAdtoDischarge <= 48*60 -- 14,020
-- 
-- select count(distinct patient.subject_id) FROM chd_discharge cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (chd = 1) and timeAdtoDischarge <= 48*60 -- 10,417 


-- -- CHF with Time and Age filters:
-- 
-- select count(distinct patient.subject_id) FROM chd_discharge cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (chd = 1) and timeAdtoDischarge <= 48*60 and CHD_CLASS = 'SYSTOLIC' -- 137
-- 
-- select count(*) FROM chd_discharge cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (chd = 1) and timeAdtoDischarge <= 48*60 and CHD_CLASS = 'SYSTOLIC' --  140
-- 
-- select count(distinct patient.subject_id)  FROM chd_discharge cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (chd = 1) and timeAdtoDischarge <= 48*60 and CHD_CLASS = 'DIASTOLIC' -- 245
    
-- select count(*) FROM chd_discharge cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (chd = 1) and timeAdtoDischarge <= 48*60 and CHD_CLASS = 'DIASTOLIC' -- 254
-- 
--select count(*) FROM chd_discharge cohort
--left join cohort_patient_demographics patient
--on cohort.subject_id = patient.subject_id
--and cohort.hadm_id = patient.hadm_id
--where age_flag = 1 and (chd = 1) and timeAdtoDischarge <= 24*60 and CHD_CLASS = 'COMBINED'; -- 49
---- 
-- select count(distinct patient.subject_id)  FROM chd_discharge cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (chd = 1) and timeAdtoDischarge <= 48*60 and CHD_CLASS = 'COMBINED' -- 49
-- 
-- 
-- select count(*) FROM chd_discharge cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (chd = 1) and timeAdtoDischarge <= 48*60 and CHD_CLASS = 'UNSPECIFIED' -- 12,982
-- 
-- select count(distinct patient.subject_id)  FROM chd_discharge cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (chd = 1) and timeAdtoDischarge <= 48*60 and CHD_CLASS = 'UNSPECIFIED' -- 9,778 
-- 
-- 
-- select count(*) FROM chd_discharge cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (chd = 1) and timeAdtoDischarge <= 48*60 and CHD_CLASS = 'DRG' -- 595
-- 
-- select count(distinct patient.subject_id)  FROM chd_discharge cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (chd = 1) and timeAdtoDischarge <= 48*60 and CHD_CLASS = 'DRG' -- 555 


---------------------------------------------------------------------------------------------------------
 -- Create initial cohort: First encounter (ind =1) of each ICU admission, with sepsis (angus =1) 
 -- and heart failure (chd =1) tableau_initialsepsiscohort.sql
 -- flags for DNI, End stage renal therapy are calculated here.
---------------------------------------------------------------------------------------------------------

set search_path to public, mimiciii;

--select * from admissions
drop table  if exists SepsisCHFCohorts;
create table SepsisCHFCohorts as 
(
    SELECT 
        admissionindex.subject_id, admissionindex.hadm_id, admissionindex.icustay_id, admissionindex.ind,
        --angus.subject_id,
        angus.infection, angus.explicit_sepsis, angus.organ_dysfunction, 
        angus.mech_vent, angus.angus, angus.chd, angus.chd_class,
        admissions.row_id, 
        --admissions.subject_id, admissions.hadm_id, 
        admissions.admittime, 
        admissions.dischtime, admissions.deathtime, admissions.admission_type, admissions.admission_location, 
        admissions.discharge_location, admissions.insurance, admissions.language, admissions.religion, admissions.marital_status,
        admissions.ethnicity, admissions.edregtime, admissions.edouttime, admissions.diagnosis, 
        admissions.hospital_expire_flag, admissions.has_chartevents_data, 
        EXTRACT(day FROM admissions.dischtime - admissions.admittime)*24+EXTRACT(HOUR FROM admissions.dischtime - admissions.admittime)  as timeAdtoDischarge,
        EXTRACT(day FROM admissions.deathtime - admissions.admittime)*24+EXTRACT(HOUR FROM admissions.deathtime - admissions.admittime)  as timeAdtoDeath
        FROM mimiciii_angus_sepsis_tbt  angus

        LEFT JOIN mimiciii_admissions_index admissionindex
        ON admissionindex.hadm_id = angus.hadm_id and angus.subject_id = admissionindex.subject_id
        LEFT JOIN admissions admissions
        ON admissionindex.hadm_id = admissions.hadm_id and admissions.subject_id = admissionindex.subject_id
        WHERE (angus = 1 or (angus = 1 and chd = 1)) and ind = 1 
    );

   

-- 
-- -- INCLUSION AND EXCLUSION CRITERIA --
-- 
-- -- sepsis --
-- 
-- -- Time filter on sepsis:
-- select count(*) from SepsisCHFCohorts where angus = 1 and timeAdtoDischarge <= 24*60; -- 14,919 
-- select count(distinct subject_id) from SepsisCHFCohorts where angus = 1 and timeAdtoDischarge <= 24*60; -- 12,403  
---- 
-- select count(*) from SepsisCHFCohorts where angus = 1 and timeAdtoDischarge <= 48*60; -- 15,213
-- select count(distinct subject_id) from SepsisCHFCohorts where angus = 1 and timeAdtoDischarge <= 48*60; -- 12,604 
---- 
---- --  Age filter on sepsis:
-- select count(*) FROM SepsisCHFCohorts cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (angus = 1); -- 15,103 
---- 
-- select count(distinct patient.subject_id) FROM SepsisCHFCohorts cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (angus = 1); -- 12,487 
---- 
---- 
---- -- Age and time filters on sepsis
-- select count(*) FROM SepsisCHFCohorts cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (angus = 1) and timeAdtoDischarge <= 24*60; -- 14,841 
---- 
-- select count(distinct patient.subject_id) FROM SepsisCHFCohorts cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (angus = 1) and timeAdtoDischarge <= 24*60; -- 12,326 
---- 
-- select count(*) FROM SepsisCHFCohorts cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (angus = 1) and timeAdtoDischarge <= 48*60; -- 15,075 
---- 
-- select count(distinct patient.subject_id) FROM SepsisCHFCohorts cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (angus = 1) and timeAdtoDischarge <= 48*60 -- 12,468 
-- 
-- 
-- -- sepsis and chf --
--
-- select count(*) from SepsisCHFCohorts where (angus = 1 and chd = 1); -- 5,774 
-- select count(distinct subject_id) from SepsisCHFCohorts where (angus = 1 and chd = 1); -- 4,791  
-- 
-- 
-- -- Time filter on sepsis+chf
 -- select count(*) from SepsisCHFCohorts where (angus = 1 and chd = 1) and timeAdtoDischarge <= 24*60; -- 5,703 
 -- select count(distinct subject_id) from SepsisCHFCohorts where (angus = 1 and chd = 1) and timeAdtoDischarge <= 24*60; -- 4,738   
 
-- select count(*) from SepsisCHFCohorts where (angus = 1 and chd = 1)  and timeAdtoDischarge <= 48*60; -- 5,770 
-- select count(distinct subject_id) from SepsisCHFCohorts where (angus = 1 and chd = 1)  and timeAdtoDischarge <= 48*60; -- 4,788 
-- 
-- 
-- -- Age filter on sepsis+chf
-- select count(*) FROM SepsisCHFCohorts cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (angus = 1 and chd = 1); -- 5,772
-- 
-- 
-- select count(distinct cohort.subject_id) 
-- FROM SepsisCHFCohorts cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (angus = 1 and chd = 1) -- 4,789 
-- 
-- 
-- -- Age and time filters on sepsis+chf
-- select count(*) FROM SepsisCHFCohorts cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (angus = 1 and chd = 1) and timeAdtoDischarge <= 24*60; -- 5,701 
-- 
-- select count(distinct patient.subject_id) FROM SepsisCHFCohorts cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (angus = 1 and chd = 1) and timeAdtoDischarge <= 24*60; -- 4,736  
-- 
-- select count(*) FROM SepsisCHFCohorts cohort
-- left join cohort_patient_demographics patient
-- on cohort.subject_id = patient.subject_id
-- and cohort.hadm_id = patient.hadm_id
-- where age_flag = 1 and (angus = 1 and chd = 1) and timeAdtoDischarge <= 48*60; -- 5,768
-- 
 select count(distinct patient.subject_id) FROM SepsisCHFCohorts cohort
 left join cohort_patient_demographics patient
 on cohort.subject_id = patient.subject_id
 and cohort.hadm_id = patient.hadm_id
 where age_flag = 1 and (angus = 1 and chd = 1) and timeAdtoDischarge <= 48*60; -- 4,786

---------------------------------------------------------------------------------------------------------
 --  Create flag for DNI and DNR -- code_status.sql
---------------------------------------------------------------------------------------------------------

DROP Table IF EXISTS DNIDNR;
CREATE Table DNIDNR AS
with t1 as
(
  select icustay_id, charttime, value
  -- use row number to identify first and last code status
  , ROW_NUMBER() over (PARTITION BY icustay_id order by charttime) as rnFirst
  , ROW_NUMBER() over (PARTITION BY icustay_id order by charttime desc) as rnLast
  -- coalesce the values
  , case
      when value in ('Full Code','Full code') then 1
    else 0 end as FullCode
  , case
      when value in ('Comfort Measures','Comfort measures only') then 1
    else 0 end as CMO
  , case
      when value = 'CPR Not Indicate' then 1
    else 0 end as DNCPR -- only in CareVue, i.e. only possible for ~60-70% of patients
  , case
      when value in ('Do Not Intubate','DNI (do not intubate)','DNR / DNI') then 1
    else 0 end as DNI
  , case
      when value in ('Do Not Resuscita','DNR (do not resuscitate)','DNR / DNI') then 1
    else 0 end as DNR
  from chartevents
  where itemid in (128, 223758)
  and value is not null
  and value != 'Other/Remarks'
  -- exclude rows marked as error
  AND error IS DISTINCT FROM 1
)
-- examine the discharge summaries to determine if they were ever made cmo
, disch as
(
  select
    ne.hadm_id
    , max(case
        when substring(substring(text from '[^E]CMO') from 2 for 3) = 'CMO'
          then 1
        else 0
      end) as CMO
    --
    -- , case
    --     when substring(text from '^[E]CMO') as CMO
  from noteevents ne
  where category = 'Discharge summary'
  and text like '%CMO%'
  group by hadm_id
)
-- examine the notes to determine if they were ever made cmo
, nnote as
(
  select
    hadm_id, charttime
    , max(case
        when substring(text from 'made CMO') != '' then 1
        when substring(lower(text) from 'cmo ordered') != '' then 1
        when substring(lower(text) from 'pt. is cmo') != '' then 1
        when substring(text from 'Code status:([ \r\n]+)Comfort measures only') != '' then 1
        --when substring(text from 'made CMO') != '' then 1
        --when substring(substring(text from '[^E]CMO') from 2 for 3) = 'CMO'
        --  then 1
        else 0
      end) as CMO
  from noteevents ne
  where category in ('Nursing/other','Nursing','Physician')
  and lower(text) like '%cmo%'
  group by hadm_id, charttime
)
select ie.subject_id, ie.hadm_id, ie.icustay_id
  -- first recorded code status
  , max(case when rnFirst = 1 then t1.FullCode else null end) as FullCode_first
  , max(case when rnFirst = 1 then t1.CMO else null end) as CMO_first
  , max(case when rnFirst = 1 then t1.DNR else null end) as DNR_first
  , max(case when rnFirst = 1 then t1.DNI else null end) as DNI_first
  , max(case when rnFirst = 1 then t1.DNCPR else null end) as DNCPR_first
  -- last recorded code status
  , max(case when  rnLast = 1 then t1.FullCode else null end) as FullCode_last
  , max(case when  rnLast = 1 then t1.CMO else null end) as CMO_last
  , max(case when  rnLast = 1 then t1.DNR else null end) as DNR_last
  , max(case when  rnLast = 1 then t1.DNI else null end) as DNI_last
  , max(case when  rnLast = 1 then t1.DNCPR else null end) as DNCPR_last
  -- were they *at any time* given a certain code status
  , max(t1.FullCode) as FullCode
  , max(t1.CMO) as CMO
  , max(t1.DNR) as DNR
  , max(t1.DNI) as DNI
  , max(t1.DNCPR) as DNCPR
  -- discharge summary mentions CMO
  -- *** not totally robust, the note could say "NOT CMO", which would be flagged as 1
  , max(case when disch.cmo = 1 then 1 else 0 end) as CMO_ds
  -- time until their first DNR
  , min(case when t1.DNR = 1 then t1.charttime else null end)
        as TimeDNR_chart
  -- first code status of CMO
  , min(case when t1.CMO = 1 then t1.charttime else null end)
        as TimeCMO_chart
  , min(case when t1.CMO = 1 then nn.charttime else null end)
        as TimeCMO_NursingNote
from icustays ie
left join t1
  on ie.icustay_id = t1.icustay_id
left join nnote nn
  on ie.hadm_id = nn.hadm_id and nn.charttime between ie.intime and ie.outtime
left join disch
  on ie.hadm_id = disch.hadm_id
group by ie.subject_id, ie.hadm_id, ie.icustay_id, ie.intime;

-- select * from SepsisCHFCohorts where flag_DNI =  1

-- select count(*) from DNIDNR where dncpr_first = 1 -- 17

-- select * from DNIDNR where dncpr_first = 1 and  dnr = 1

-- select * from noteevents where subject_id in(
-- select subject_id from DNIDNR where dncpr_first = 1 and  dnr = 1) 
-- order by subject_id -- 5

-- select * from noteevents  fetch first 2 rows only

-- select count(*) from DNIDNR where dncpr_last = 1 -- 92

-- select count(*) from DNIDNR where dncpr_last = 1 and  dnr = 1 -- 13

-- select count(*) from DNIDNR where dnr = 1 -- 5699

---------------------------------------------------------------------------------------------------------
-- Set flag with DNI, DNR and DNCPR for table SepsisCHFCohorts
---------------------------------------------------------------------------------------------------------

ALTER TABLE SepsisCHFCohorts drop column flag_DNIDNR

ALTER TABLE SepsisCHFCohorts
ADD COLUMN flag_DNIDNR int default 0;

UPDATE SepsisCHFCohorts SET flag_DNIDNR = 1
 from  DNIDNR 
    where (DNIDNR.dnr = 1
    or DNIDNR.dnr_first = 1
    or DNIDNR.dnr_last = 1
    or DNIDNR.dni = 1  
    or DNIDNR.dni_first = 1  
    or DNIDNR.dni_last = 1  
    or DNIDNR.dncpr_first = 1
    or DNIDNR.dncpr_last = 1
        ) and 
 SepsisCHFCohorts.hadm_id = DNIDNR.hadm_id
AND SepsisCHFCohorts.subject_id = DNIDNR.subject_id;

-- select count(*) from SepsisCHFCohorts where flag_dnidnr = 1 -- 3084


---------------------------------------------------------------------------------------------------------
 -- END-STAGE RENAL FAILURE
---------------------------------------------------------------------------------------------------------

-- CASE -- EXCLUDE PATIENTS FROM THE STUDY --
-- when icd9_code = '40403' then 1
-- when icd9_code = '40413' then 1
-- when icd9_code = '40493' then 1 /* Hypertensive heart and renal disease with heart and renal failure */


ALTER TABLE SepsisCHFCohorts drop column flag_esrt

--End stage renal therapy
ALTER TABLE SepsisCHFCohorts
ADD COLUMN flag_esrt int default 0;

UPDATE SepsisCHFCohorts set flag_esrt =  1
from 
 ( SELECT distinct subject_id, hadm_id,icd9_code
    FROM diagnoses_icd
    where icd9_code = '5856' or icd9_code = '40403' or icd9_code = '40413' or icd9_code = '40493') as a
 where SepsisCHFCohorts.subject_id = a.subject_id and 
SepsisCHFCohorts.hadm_id = a.hadm_id

--The end:   End stage renal therapy

select * from SepsisCHFCohorts where flag_esrt =  1


---------------------------------------------------------------------------------------------------------
 -- FLUID RESUSCITATION --
---------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------
 -- CRYSTALLOIDS -- https://github.com/MIT-LCP/mimic-code/blob/dev/concepts/fluid-balance/crystalloid-bolus.sql
---------------------------------------------------------------------------------------------------------


DROP TABLE IF EXISTS crystalloid_bolus CASCADE;
CREATE TABLE crystalloid_bolus AS
with t1 as
(
  select
    mv.icustay_id
  , mv.starttime as charttime
  -- standardize the units to millilitres
  -- also metavision has floating point precision.. but we only care down to the mL
  , round(case
      when mv.amountuom = 'L'  
        then mv.amount * 1000.0
      when mv.amountuom = 'ml'
        then mv.amount
    else null end) as amount
  from inputevents_mv mv
  where mv.itemid in
  (
    -- 225943 Solution
    225158, -- NaCl 0.9%
    225828, -- LR
    225944, -- Sterile Water
    225797  -- Free Water
  )
  and mv.statusdescription != 'Rewritten'
  and
  -- in MetaVision, these ITEMIDs appear with a null rate IFF endtime=starttime + 1 minute
  -- so it is sufficient to:
  --    (1) check the rate is > 240 if it exists or
  --    (2) ensure the rate is null and amount > 240 ml
    (
      (mv.rate is not null and mv.rateuom = 'mL/hour' and mv.rate > 248)
      OR (mv.rate is not null and mv.rateuom = 'mL/min' and mv.rate > (248/60.0))
      OR (mv.rate is null and mv.amountuom = 'L' and mv.amount > 0.248)
      OR (mv.rate is null and mv.amountuom = 'ml' and mv.amount > 248)
    )
)
, t2 as
(
  select
    cv.icustay_id
  , cv.charttime
  -- carevue always has units in millilitres
  , round(cv.amount) as amount
  from inputevents_cv cv
  where cv.itemid in
  (
    30018 --    .9% Normal Saline
  , 30021 --    Lactated Ringers
  , 30058 --    Free Water Bolus
  , 40850 --    ns bolus
  , 41491 --    fluid bolus
  , 42639 --    bolus
  , 30065 --    Sterile Water
  , 42187 --    free h20
  , 43819 --    1:1 NS Repletion.
  , 30063 --    IV Piggyback
  , 41430 --    free water boluses
  , 40712 --    free H20
  , 44160 --    BOLUS
  , 42383 --    cc for cc replace
  , 30169 --    Sterile H20_GU
  , 42297 --    Fluid bolus
  , 42453 --    Fluid Bolus
  , 40872 --    free water
  , 41915 --    FREE WATER
  , 41490 --    NS bolus
  , 46501 --    H2O Bolus
  , 45045 --    WaterBolus
  , 41984 --    FREE H20
  , 41371 --    ns fluid bolus
  , 41582 --    free h20 bolus
  , 41322 --    rl bolus
  , 40778 --    Free H2O
  , 41896 --    ivf boluses
  , 41428 --    ns .9% bolus
  , 43936 --    FREE WATER BOLUSES
  , 44200 --    FLUID BOLUS
  , 41619 --    frfee water boluses
  , 40424 --    free H2O
  , 41457 --    Free H20 intake
  , 41581 --    Water bolus
  , 42844 --    NS fluid bolus
  , 42429 --    Free water
  , 41356 --    IV Bolus
  , 40532 --    FREE H2O
  , 42548 --    NS Bolus
  , 44184 --    LR Bolus
  , 44521 --    LR bolus
  , 44741 --    NS FLUID BOLUS
  , 44126 --    fl bolus
  , 44110 --    RL BOLUS
  , 44633 --    ns boluses
  , 44983 --    Bolus NS
  , 44815 --    LR BOLUS
  , 43986 --    iv bolus
  , 45079 --    500 cc ns bolus
  , 46781 --    lr bolus
  , 45155 --    ns cc/cc replacement
  , 43909 --    H20 BOlus
  , 41467 --    NS IV bolus
  , 44367 --    LR
  , 41743 --    water bolus
  , 40423 --    Bolus
  , 44263 --    fluid bolus ns
  , 42749 --    fluid bolus NS
  , 45480 --    500cc ns bolus
  , 44491 --    .9NS bolus
  , 41695 --    NS fluid boluses
  , 46169 --    free water bolus.
  , 41580 --    free h2o bolus
  , 41392 --    ns b
  , 45989 --    NS Fluid Bolus
  , 45137 --    NS cc/cc
  , 45154 --    Free H20 bolus
  , 44053 --    normal saline bolus
  , 41416 --    free h2o boluses
  , 44761 --    Free H20
  , 41237 --    ns fluid boluses
  , 44426 --    bolus ns
  , 43975 --    FREE H20 BOLUSES
  , 44894 --    N/s 500 ml bolus
  , 41380 --    nsbolus
  , 42671 --    free h2o
  )
  and cv.amount > 248
  and cv.amount <= 2000
  and cv.amountuom = 'ml'
)
select
    icustay_id
  , charttime
  , sum(amount) as crystalloid_bolus
from t1
-- just because the rate was high enough, does *not* mean the final amount was
where amount > 248
group by t1.icustay_id, t1.charttime
UNION
select
    icustay_id
  , charttime
  , sum(amount) as crystalloid_bolus
from t2
group by t2.icustay_id, t2.charttime
order by icustay_id, charttime;

---------------------------------------------------------------------------------------------------------
 -- COLLOIDS -- https://github.com/MIT-LCP/mimic-code/blob/dev/concepts/fluid-balance/colloid-bolus.sql
---------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS colloid_bolus CASCADE;
CREATE TABLE colloid_bolus AS
-- received colloid before admission
-- 226365  --  OR Colloid Intake
-- 226376  --  PACU Colloid Intake

with t1 as
(
  select
    mv.icustay_id
  , mv.starttime as charttime
  -- standardize the units to millilitres
  -- also metavision has floating point precision.. but we only care down to the mL
  , round(case
      when mv.amountuom = 'L'
        then mv.amount * 1000.0
      when mv.amountuom = 'ml'
        then mv.amount
    else null end) as amount
  from inputevents_mv mv
  where mv.itemid in
  (
    220864, --  Albumin 5%  7466 132 7466
    220862, --  Albumin 25% 9851 174 9851
    225174, --  Hetastarch (Hespan) 6%  82 1 82
    225795,  -- Dextran 40  38 3 38
    225796 --  Dextran 70
    -- below ITEMIDs not in use
   -- 220861 | Albumin (Human) 20%
   -- 220863 | Albumin (Human) 4%
  )
  and mv.statusdescription != 'Rewritten'
  and
  -- in MetaVision, these ITEMIDs never appear with a null rate
  -- so it is sufficient to check the rate is > 100
    (
      (mv.rateuom = 'mL/hour' and mv.rate > 100)
      OR (mv.rateuom = 'mL/min' and mv.rate > (100/60.0))
      OR (mv.rateuom = 'mL/kg/hour' and (mv.rate*mv.patientweight) > 100)
    )
)
, t2 as
(
  select
    cv.icustay_id
  , cv.charttime
  -- carevue always has units in millilitres (or null)
  , round(cv.amount) as amount
  from inputevents_cv cv
  where cv.itemid in
  (
   30008 -- Albumin 5%
  ,30009 -- Albumin 25%
  ,42832 -- albumin 12.5%
  ,40548 -- ALBUMIN
  ,45403 -- albumin
  ,44203 -- Albumin 12.5%
  ,30181 -- Serum Albumin 5%
  ,46564 -- Albumin
  ,43237 -- 25% Albumin
  ,43353 -- Albumin (human) 25%

  ,30012 -- Hespan
  ,46313 -- 6% Hespan

  ,42975 -- DEXTRAN DRIP
  ,42944 -- dextran
  ,46336 -- 10% Dextran 40/D5W
  ,46729 -- Dextran
  ,40033 -- DEXTRAN
  ,45410 -- 10% Dextran 40
  ,30011 -- Dextran 40
  ,30016 -- Dextrose 10%
  ,42731 -- Dextran40 10%
  )
  and cv.amount > 100
  and cv.amount < 2000
)
-- some colloids are charted in chartevents
, t3 as
(
  select
    ce.icustay_id
  , ce.charttime
  -- carevue always has units in millilitres (or null)
  , round(ce.valuenum) as amount
  from chartevents ce
  where ce.itemid in
  (
      2510 --   DEXTRAN LML 10%
    , 3087 --   DEXTRAN 40  10%
    , 6937 --   Dextran
    , 3087 -- | DEXTRAN 40  10%
    , 3088 --   DEXTRAN 40%
  )
  and ce.valuenum is not null
  and ce.valuenum > 100
  and ce.valuenum < 2000
)
select
    icustay_id
  , charttime
  , sum(amount) as colloid_bolus
from t1
-- just because the rate was high enough, does *not* mean the final amount was
where amount > 100
group by t1.icustay_id, t1.charttime
UNION
select
    icustay_id
  , charttime
  , sum(amount) as colloid_bolus
from t2
group by t2.icustay_id, t2.charttime
UNION
select
    icustay_id
  , charttime
  , sum(amount) as colloid_bolus
from t3
group by t3.icustay_id, t3.charttime
order by icustay_id, charttime;
