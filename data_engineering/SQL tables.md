# SQL tables

This file documents how we built the current BigQuery tables we have in our GCP project.

## `brca_betas_clustered_efficient`

```sql
CREATE TABLE build_hackathon_dnanyc.brca_betas_clustered_efficient
PARTITION BY RANGE_BUCKET(row_number, GENERATE_ARRAY(1, 11000, 100))
CLUSTER BY row_number
AS (
SELECT A.*, B.row_number
from
(SELECT *, CONCAT(SPLIT(aliquot_barcode, '-')[offset(0)], '-', SPLIT(aliquot_barcode, '-')[offset(1)], '-', SPLIT(aliquot_barcode, '-')[offset(2)]) as participant_id
from --build_hackathon_dnanyc.Brce_betas_breast_tissue_only_4cols) as A
build_hackathon_dnanyc.brca_betas_sample_codes_efficient) as A
join build_hackathon_dnanyc.participant_ids as B
on A.participant_id = B.participant_id
)
```

## `brca_betas_sample_codes_efficient`

```sql
WITH
  brca_betas AS (
  SELECT
    IFNULL(brca.ParticipantBarcode,
      "non-BRCA") AS BRCA_ParticipantCode,
    MIM.*,
    dna.aliquot_barcode,
    dna.probe_id,
    dna.beta_value,
    gc.Name
  FROM
    `isb-cgc.platform_reference.methylation_annotation` gc
  JOIN
    `isb-cgc.TCGA_hg38_data_v0.DNA_Methylation` dna
  ON
    gc.Name = dna.probe_id
  JOIN
    `isb-cgc.platform_reference.GDC_hg38_methylation_annotation` MIM
  ON
    MIM.CpG_probe_id = gc.Name
  LEFT JOIN
    `isb-cgc.tcga_cohorts.BRCA` brca
  ON
    dna.sample_barcode = brca.SampleBarcode )
SELECT
  beta_value,
  CpG_probe_id,
  BRCA_ParticipantCode,
  aliquot_barcode,
  SUBSTR( aliquot_barcode,14,2) AS sample_id,
IF
  (SUBSTR( aliquot_barcode,14,2) IN ('10',
      '11',
      '12',
      '13',
      '14',
      '15',
      '16',
      '17',
      '18',
      '19'),
    "normal",
  IF
    (SUBSTR( aliquot_barcode,14,2) IN ('01',
        '02',
        '03',
        '04',
        '05',
        '06',
        '07',
        '08',
        '09'),
      'tumor',
      "control")) AS sample_status
FROM
  brca_betas
```

## `patient_cancer_stage_v2` 

```sql
SELECT
  case_barcode,
  clinical_stage,
  pathologic_stage
FROM
  `isb-cgc.TCGA_bioclin_v0.Clinical`
```

## `patient_cancer_stage_v3`

```sql
SELECT
  case_barcode,
  clinical_stage,
  clinical_T,
  clinical_N,
  clinical_M,
  pathologic_stage,
  pathologic_T,
  pathologic_N,
  pathologic_M
FROM
  `isb-cgc.TCGA_bioclin_v0.Clinical`
UNION ALL
SELECT
  case_barcode,
  clinical_stage,
  clinical_T,
  clinical_N,
  clinical_M,
  pathologic_stage,
  pathologic_T,
  pathologic_N,
  pathologic_M
FROM
  `isb-cgc.TCGA_bioclin_v0.clinical_v1`
```

## `patient_cancer_stage_v4`

```sql
SELECT
  case_barcode,
  project_short_name,
  clinical_stage,
  clinical_T,
  clinical_N,
  clinical_M,
  pathologic_stage,
  pathologic_T,
  pathologic_N,
  pathologic_M
FROM
  `isb-cgc.TCGA_bioclin_v0.Clinical`
UNION ALL
SELECT
  case_barcode,
  project_short_name,
  clinical_stage,
  clinical_T,
  clinical_N,
  clinical_M,
  pathologic_stage,
  pathologic_T,
  pathologic_N,
  pathologic_M
FROM
  `isb-cgc.TCGA_bioclin_v0.clinical_v1`
```