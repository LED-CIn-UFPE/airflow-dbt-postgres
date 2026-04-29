FROM astrocrpublic.azurecr.io/runtime:3.0-2

ENV DBT_TARGET_PATH=/tmp/dbt/target
ENV DBT_LOG_PATH=/tmp/dbt/logs
ENV DBT_PACKAGES_INSTALL_PATH=/tmp/dbt/dbt_packages

RUN pip install --no-cache-dir dbt-postgres