import apache_beam

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

QUERY = """
SELECT *
from `dna_cancer_prediction.test_tcga_betas`
"""

BUCKET = 'dna-methylation-cancer'

pipeline_options = PipelineOptions(
    # runner='DataflowRunner',
    project='gcp-nyc',
    # job_name='unique-job-name',
    temp_location=f'gs://{BUCKET}/temp',
    region='us-central1')

with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    read_gbq = p | 'Read' >> beam.io.Read(beam.io.BigQuerySource(query=QUERY,
                                                                 use_standard_sql=True))
    aliquot_count = (read_gbq
                     | 'ExtractAliquot' >> beam.Map(lambda elt: elt['aliquot_barcode'])
                     | 'Deduplicate' >> beam.Distinct()
                     | 'ReplaceWithOnes' >> beam.Map(lambda elt: 1)
                     | 'Sum' >> beam.CombineGlobally(sum)
                     )

    cpg_observation_count = (read_gbq
                             | 'ExtractCpG' >> beam.Map(lambda elt: (elt['CpG_probe_id'], 1))
                             | 'CombineCpG' >> beam.CombinePerKey(sum)
                             )
    # counts = (
    #     lines
    #     | 'Split' >>
    #     (beam.ParDo(WordExtractingDoFn()).with_output_types(unicode))
    #     | 'PairWIthOne' >> beam.Map(lambda x: (x, 1))
    #     | 'GroupAndSum' >> beam.CombinePerKey(sum))
    #

    cpg_observation_count | 'Write' >> beam.io.WriteToText(f'gs://{BUCKET}/test', file_name_suffix='.csv')

