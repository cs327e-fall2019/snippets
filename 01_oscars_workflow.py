import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator


default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 11, 25)
}

bq_query_start = 'bq query --use_legacy_sql=false '

sql_actors = ''' create or replace table oscars.Winning_Actors as 
                 select entity as name, year, category 
                 from oscars.Nomination_Events 
                 where winner = true
                 and category like '%ACTOR%' ''' 

sql_actresses = ''' create or replace table oscars.Winning_Actresses as 
                 select entity as name, year, category 
                 from oscars.Nomination_Events 
                 where winner = true
                 and category like '%ACTRESS%' ''' 

with models.DAG(
        '01_oscars_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    winning_actors = BashOperator(
            task_id='winning_actors',
            bash_command=bq_query_start + '"' + sql_actors + '"')
    
    winning_actresses = BashOperator(
            task_id='winning_actresses',
            bash_command=bq_query_start + '"' + sql_actresses + '"')
        
    winning_actors >> winning_actresses