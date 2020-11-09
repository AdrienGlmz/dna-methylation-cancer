# Custom TFX Component TCGA Processing

# Introduction
This repository builds a fully custom 
[TensorFlow Extended (TFX)](https://tensorflow.org/tfx) component based on this 
[documentation](https://www.tensorflow.org/tfx/guide/custom_componen). The components
reads the raw TCGA data in BigQuery, applies an Apache Beam PTransform pipeline and
output them out in another BigQuery table.

As this component will be part in a TFX pipeline and will be executed by the AI Platform DAG 
runner and run with the Dataflow runner, this component was designed as a Python
[namespace package](https://packaging.python.org/guides/packaging-namespace-packages/) 
to be integrated into the `tfx.extensions` package.

## Prerequisites

* Linux or MacOS
* Python 3.6+
* Git

### Required packages
* [TFX](https://pypi.org/project/tfx/)

This component has been tested with `TFX==0.23.0`

### How to install

While in the folder containing this README (i.e. in `tcga_preprocessing_component/`), run
```
pip install .
```

This will install the freewheel_preprocessing_component subpackages.

### How to run

Once installed and within Python, you'll be able to import the package using
```python
from tfx.extensions.tcga_preprocessing import component as tcga_reader
```

To use this component, do
```python
tcga_reader = tcga_reader.TCGAPreprocessing(query=query,
                                            bucket_name=bucket_name,
                                            beam_transform=freewheel_ptransform,
                                            output_schema=freewheel_output_schema,
                                            table_name=freewheel_output_table_name)
```
where:
  - `query` is a `string` containing the input BigQuery query reading the raw freewheel logs
  - `bucket_name` is a `string` containing the name of the GCS bucket to use as temporary storage space
  - `beam_transform` is a `beam.PTransform` object that will preprocessing the raw TCGA data and create the 
  necessary columns for the output
  - `table_name` is a `string` containing the BigQuery table where the output will be exported.