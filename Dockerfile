FROM jupyter/scipy-notebook as jupyter
RUN pip install pandas
RUN pip install psycopg2-binary