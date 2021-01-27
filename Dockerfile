FROM puckel/docker-airflow:1.10.9

COPY airflow/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
COPY entry_point.sh /
#RUN chmod +x /entry_point.sh
#CMD ["/bin/bash", "/entry_point.sh"]
