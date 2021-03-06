FROM python:3.8.6
WORKDIR /root

RUN pip install xgboost==1.2.1 scikit-learn==0.23.2 pandas==1.1.3 joblib==0.17.0
RUN pip install cloudml-hypertune
RUN pip install google-cloud-storage==1.32.0

# Installs google cloud sdk, this is mostly for using gsutil to export model.
RUN wget -nv \
    https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz && \
    mkdir /root/tools && \
    tar xvzf google-cloud-sdk.tar.gz -C /root/tools && \
    rm google-cloud-sdk.tar.gz && \
    /root/tools/google-cloud-sdk/install.sh --usage-reporting=false \
        --path-update=false --bash-completion=false \
        --disable-installation-options && \
    rm -rf /root/.config/* && \
    ln -s /root/.config /config && \
    # Remove the backup directory that gcloud creates
    rm -rf /root/tools/google-cloud-sdk/.install/.backup

# Path configuration
ENV PATH $PATH:/root/tools/google-cloud-sdk/bin
# Make sure gsutil will use the default service account
RUN echo '[GoogleCompute]\nservice_account = default' > /etc/boto.cfg

# Copies the trainer code
RUN mkdir /root/trainer
COPY ./ai-platform-training/trainer /root/trainer
COPY ./configs /root/trainer/configs


# Sets up the entry point to invoke the trainer.
ENTRYPOINT ["python", "trainer/xgboost-binary-model.py"]
