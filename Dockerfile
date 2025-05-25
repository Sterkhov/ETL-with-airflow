FROM apache/airflow:3.0.1

USER root

# Install dependencies for ODBC
RUN apt-get update && apt-get install -y curl apt-transport-https gnupg2 \
    curl \
    apt-transport-https \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 msodbcsql17\
    && apt-get install -y unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install pyodbc and MSSQL provider
RUN pip install --no-cache-dir apache-airflow-providers-microsoft-mssql pyodbc
    