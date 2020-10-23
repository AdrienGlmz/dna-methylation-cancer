FROM tensorflow/tfx:0.23.0
WORKDIR /pipeline
COPY ./ ./

# Copy tcga custom components files for the Dataflow Runner
COPY ./tcga_preprocessing_component/tfx/extensions/tcga_preprocessing /tfx-src/tfx/extensions/tcga_preprocessing
# Install tcga_preprocessing_componentn package for the AI Platform DAG Runner
RUN cd ./tcga_preprocessing_component && pip install .
RUN pip install google-cloud-storage==1.31.0 google-cloud-bigquery==1.28.0

ENV PYTHONPATH="/pipeline:${PYTHONPATH}"