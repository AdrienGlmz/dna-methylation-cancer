#BigQuery SQL

# solid cancer tissue samples (identified by unique aliquot barcodes)
SELECT aliquot_barcode, probe_id, beta_value
FROM `isb-cgc.TCGA_hg38_data_v0.DNA_Methylation`
WHERE #aliquot_barcode = 'TCGA-12-1601-01A-01D-0595-05' )
#WHERE
probe_id = 'cg13412283'

#A subset of samples and CpG probes were selected for use in the project, such that each included sample would contain
#methylation betas for the same set of CpG probes. This step reduced the size of the research dataset to 8,126 aliquots, with the
#same 323,179 probes included with each aliquot.

SELECT brca.ParticipantBarcode, gc.UCSC, MIM.*, dna.aliquot_barcode, dna.probe_id, dna.beta_value, gc.Name
FROM `isb-cgc.platform_reference.methylation_annotation` gc
JOIN  `isb-cgc.TCGA_hg38_data_v0.DNA_Methylation` dna ON gc.Name = dna.probe_id
JOIN `isb-cgc.platform_reference.GDC_hg38_methylation_annotation` MIM on MIM.CpG_probe_id = gc.Name
LEFT JOIN `isb-cgc.tcga_cohorts.BRCA` brca ON dna.aliquot_barcode = brca.SampleBarcode
where gc.Name = 'cg00332146'
# use tcga_cohorts BRCAUCSC
