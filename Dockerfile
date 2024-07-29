FROM python:3.9
RUN echo "Starting Build Process"
LABEL authors="T199811E"

COPY download_files.py download_files.py
COPY etab_main.py etab_main.py
COPY legal_main.py legal_main.py
COPY utils.py utils.py
COPY main.py main.py

# set up args
ARG aws-access-key-id-data-services
ARG aws-secret-key-data-services
ARG aws-region
ARG aws-tdsynnex-sftp-bucket-url

ARG aws-access-key-id-original-tenant
ARG aws-secret-key-original-tenant

ARG preprod-admin-user
ARG preprod-admin-pass
ARG preprod-database
ARG preprod-host

ENV aws-access-key-id-data-services=$(aws-access-key-id-data-services)
ENV aws-secret-key-data-services=$(aws-secret-key-data-services)
ENV aws-region=$(aws-region)
ENV aws-tdsynnex-sftp-bucket-url=$(aws-tdsynnex-sftp-bucket-url)

ENV aws-access-key-id-original-tenant=$(aws-access-key-id-original-tenant)
ENV aws-secret-key-original-tenant=$(aws-secret-key-original-tenant)


ENV preprod-admin-user=$(preprod-admin-user)

ENV preprod-admin-pass=$(preprod-admin-pass)
ENV preprod-database=$(preprod-database)
ENV preprod-host=$(preprod-host)

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

CMD python main.py