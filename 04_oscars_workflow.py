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
                 and category like "%ACTOR%" ''' 

sql_actresses = ''' create or replace table oscars.Winning_Actresses as 
                 select entity as name, year, category 
                 from oscars.Nomination_Events 
                 where winner = true
                 and category like "%ACTRESS%" ''' 

sql_boy_names = ''' create or replace table oscars.Boy_Names as
                select name, year, sum(number) as name_count
                from `bigquery-public-data.usa_names.usa_1910_current`
                where gender = "M"
                group by name, year '''


sql_girl_names = ''' create or replace table oscars.Girl_Names as
                 select name, year, sum(number) as name_count
                 from `bigquery-public-data.usa_names.usa_1910_current`
                 where gender = "F"
                 group by name, year '''

sql_actor_name_count = ''' create or replace table oscars.Winning_Actors_Name_Count as
                        select w.fname, w.lname, w.year, w.category, g.name_count
                        from oscars.Winning_Actors_Beam w join oscars.Boy_Names g 
                        on w.fname = g.name and w.year = g.year
                       '''

sql_actress_name_count = ''' create or replace table oscars.Winning_Actress_Name_Count as
                        select w.fname, w.lname, w.year, w.category, g.name_count
                        from oscars.Winning_Actresses_Beam w join oscars.Girl_Names g 
                        on w.fname = g.name and w.year = g.year
                       '''

with models.DAG(
        '04_oscars_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    winning_actors = BashOperator(
            task_id='winning_actors',
            bash_command=bq_query_start + "'" + sql_actors + "'")
    
    winning_actresses = BashOperator(
            task_id='winning_actresses',
            bash_command=bq_query_start + "'" + sql_actresses + "'")
    
    boy_names = BashOperator(
            task_id='boy_names',
            bash_command=bq_query_start + "'" + sql_boy_names + "'", 
            trigger_rule='all_success')
    
    girl_names = BashOperator(
            task_id='girl_names',
            bash_command=bq_query_start + "'" + sql_girl_names + "'", 
            trigger_rule='all_success')
    
    parse_actor_names = BashOperator(
            task_id='parse_actor_names',
            bash_command='python /home/jupyter/airflow/dags/oscars_Winning_Actors.py')
    
    parse_actress_names = BashOperator(
            task_id='parse_actress_names',
            bash_command='python /home/jupyter/airflow/dags/oscars_Winning_Actresses.py')
    
    actor_name_count = BashOperator(
            task_id='actor_name_count',
            bash_command=bq_query_start + "'" + sql_actor_name_count + "'", 
            trigger_rule='all_success')
    
    actress_name_count = BashOperator(
            task_id='actress_name_count',
            bash_command=bq_query_start + "'" + sql_actress_name_count + "'", 
            trigger_rule='all_success')
        
    winning_actors >> boy_names >> parse_actor_names >> actor_name_count
    winning_actresses >> girl_names >> parse_actress_names >> actress_name_count