from urllib.parse import parse_qsl
import apache_beam as beam
from apache_beam.transforms.combiners import TupleCombineFn


FREEWHEEL_OUTPUT_TABLE = 'dna_cancer_prediction.test_tcga_betas'

RAW_QUERY = """
WITH
  brca_betas AS (
  SELECT
    MIM.CpG_probe_id,
    dna.case_barcode,
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
    MIM.CpG_probe_id = gc.Name),

  participants_id AS (
  SELECT
    DISTINCT case_barcode
  FROM
    `isb-cgc.TCGA_hg38_data_v0.DNA_Methylation`),

  participants_id_numbered AS (
  SELECT
    case_barcode,
    ROW_NUMBER() OVER (order by case_barcode) as row_number
  FROM
    participants_id)

SELECT
  A.case_barcode,
  A.beta_value,
  A.CpG_probe_id,
  A.aliquot_barcode,
  SUBSTR(A.aliquot_barcode, 14, 2) AS sample_id,
  IF
  (SUBSTR(A.aliquot_barcode, 14, 2) IN ('10', '11', '12', '13', '14', '15', '16', '17', '18', '19'), "normal",
  IF
  (SUBSTR(A.aliquot_barcode, 14, 2) IN ('01', '02', '03', '04',  '05', '06', '07', '08', '09'), 'tumor', "control")) AS sample_status,
  B.row_number
FROM
  brca_betas A
LEFT JOIN
  participants_id_numbered B
ON A.case_barcode = B.case_barcode
LIMIT 100
"""


OUTPUT_SCHEMA_STRING = "case_barcode:STRING, beta_value:FLOAT, CpG_probe_id:STRING, aliquot_barcode:STRING, " \
                       "sample_id:STRING, sample_status:STRING, row_number:INTEGER"


class UnnestCoGrouped(beam.DoFn):
    """This DoFn class unnests the CogroupBykey output and emits """

    def process(self, input_element, source_pipeline_name, join_pipeline_name):
        group_key, grouped_dict = input_element
        join_dictionary = grouped_dict[join_pipeline_name]
        source_dictionaries = grouped_dict[source_pipeline_name]
        for source_dictionary in source_dictionaries:
            try:
                source_dictionary.update(join_dictionary[0])
                yield source_dictionary
            except IndexError:  # found no join_dictionary
                yield source_dictionary


def keep_only_non_nulls_request_kv(elt):
    """
    Discard rows that do not contains values in the AllRequestKv Column
    """
    if elt['AllRequestKv']:
        yield elt


def keep_only_non_nulls_freewheelids(elt):
    if elt.get('FreewheelId'):
        yield elt


def parse_all_request_kv(request):
    """
    Extract FreewheelId from string of HTML parameters
    """
    request_dict = dict(parse_qsl(request))
    return request_dict.get('am_crmid')


def add_event_indicators(elt):
    """
    Indicator if Ad started and if Ad ended
    """
    elt['AdStarted'] = int(elt.get('EventName') == 'defaultImpression')
    elt['AdEnded'] = int(elt.get('EventName') == '_q_complete')
    return elt


def sum_with_null(values):
    """
    Sum while ignoring null values
    """
    return sum(filter(None, values))


def divide_with_null(a, b):
    return a / b if b != 0 else None


def divide_and_add_with_null(a, b):
    return (1 - a / b) if b != 0 else None


class SourceTransformation(beam.PTransform):
    def expand(self, pcollection):
        return (pcollection
                | 'FormatSourceData' >> beam.Map(lambda elt: (elt['UniqueIdentifier'], {'EventName': elt['EventName'],
                                                                                        'AdDuration': elt['AdDuration'],
                                                                                        'AdsSelected': elt[
                                                                                            'AdsSelected'],
                                                                                        'MaxAds': elt['MaxAds'],
                                                                                        'MaxDuration': elt[
                                                                                            'MaxDuration']}
                                                              ))
                )


class JoinTransformation(beam.PTransform):
    def expand(self, pcollection):
        return (pcollection
                | 'FilterNAs' >> beam.ParDo(keep_only_non_nulls_request_kv)
                | 'ExtractCrmId' >> beam.Map(lambda elt: (elt['UniqueIdentifier'],
                                                          parse_all_request_kv(elt['AllRequestKv'])))
                | 'Deduplicate' >> beam.Distinct()
                | 'FormatJoinData' >> beam.Map(lambda elt: (elt[0], {'FreewheelId': elt[1]}))
                )


class FinalTransformation(beam.PTransform):
    def __init__(self, source_pipeline_name, join_pipeline_name):
        self.source_pipeline_name = source_pipeline_name
        self.join_pipeline_name = join_pipeline_name

    def expand(self, pcollection):
        return (pcollection
            | 'LeftJoin' >> beam.CoGroupByKey()
            | 'UnnestCoGrouped' >> beam.ParDo(UnnestCoGrouped(), self.source_pipeline_name, self.join_pipeline_name)
            | 'AddEventIndicators' >> beam.Map(add_event_indicators)
            | 'Discard null freewheel ids' >> beam.ParDo(keep_only_non_nulls_freewheelids)
            | 'FormatJoinedData' >> beam.Map(lambda elt: (elt['FreewheelId'], (elt['MaxAds'], elt['AdsSelected'],
                                                                               elt['AdDuration'], elt['MaxDuration'],
                                                                               elt['AdStarted'], elt['AdEnded'])))
            | 'GroupBy' >> beam.CombinePerKey(TupleCombineFn(sum_with_null, sum_with_null, sum_with_null, sum_with_null,
                                                             sum_with_null, sum_with_null))
            | 'FormatResults' >> beam.Map(lambda elt: {'FreewheelID': elt[0], 'MaxAds': elt[1][0],
                                                       'AdsSelected': elt[1][1], 'AdDuration': elt[1][2],
                                                       'MaxDuration': elt[1][3], 'AdStarted': elt[1][4],
                                                       'AdEnded': elt[1][5],
                                                       'PercentExitAdBreaks': divide_and_add_with_null(elt[1][5],
                                                                                                       elt[1][4]),
                                                       'AdServedVSAdRequested': divide_with_null(elt[1][1],
                                                                                                 elt[1][0])})
                )


class FreewheelBeamPTransform(beam.PTransform):
    def expand(self, pcollection):
        source_pipeline = (pcollection | SourceTransformation())
        join_pipeline = (pcollection | JoinTransformation())

        source_pipeline_name = 'source_data'
        join_pipeline_name = 'join_data'
        pipeline_dict = {source_pipeline_name: source_pipeline,
                         join_pipeline_name: join_pipeline}
        final_pipeline = (pipeline_dict
                          | FinalTransformation(source_pipeline_name, join_pipeline_name))
        return final_pipeline
