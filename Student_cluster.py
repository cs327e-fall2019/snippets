import os, datetime, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class FormatDOBFn(beam.DoFn):
  def process(self, element):
    student_record = element
    sid = student_record.get('sid')
    fname = student_record.get('fname')
    lname = student_record.get('lname')
    dob = student_record.get('dob')
    print('current dob: ' + dob)

    # reformat any dob values that are MM/DD/YYYY to YYYY-MM-DD
    split_date = dob.split('/')
    if len(split_date) > 1:
        month = split_date[0]
        day = split_date[1]
        year = split_date[2]
        dob = year + '-' + month + '-' + day
        print('new dob: ' + dob)
        student_record['dob'] = dob
    
    # create key, value pairs
    student_tuple = (sid, student_record)
    return [student_tuple]

class DedupStudentRecordsFn(beam.DoFn):
  def process(self, element):
     sid, student_obj = element # student_obj is an _UnwindowedValues type
     student_list = list(student_obj) # cast to list to extract record
     student_record = student_list[0] # grab first student record
     print('student_record: ' + str(student_record))
     return [student_record]  
           
def run():         
    PROJECT_ID = 'cs327e-fa2019'
    BUCKET = 'gs://cs327e-beam-bucket'
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    # run pipeline on Dataflow 
    options = {
        'runner': 'DataflowRunner',
        'job_name': 'transform-student',
        'project': PROJECT_ID,
        'temp_location': BUCKET + '/temp',
        'staging_location': BUCKET + '/staging',
        'machine_type': 'n1-standard-4', # https://cloud.google.com/compute/docs/machine-types
        'num_workers': 1
    }

    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    p = beam.Pipeline('DataflowRunner', options=opts)

    sql = 'SELECT sid, fname, lname, dob FROM college_workflow_modeled.Student'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # write PCollection to log file
    query_results | 'Write log 1' >> WriteToText(DIR_PATH + 'query_results.txt')

    # apply ParDo to format the student's date of birth  
    formatted_dob_pcoll = query_results | 'Format DOB' >> beam.ParDo(FormatDOBFn())

    # write PCollection to log file
    formatted_dob_pcoll | 'Write log 2' >> WriteToText(DIR_PATH + 'formatted_dob_pcoll.txt')

    # group students by sid
    grouped_student_pcoll = formatted_dob_pcoll | 'Group by sid' >> beam.GroupByKey()

    # write PCollection to log file
    grouped_student_pcoll | 'Write log 3' >> WriteToText(DIR_PATH + 'grouped_student_pcoll.txt')

    # remove duplicate student records
    distinct_student_pcoll = grouped_student_pcoll | 'Dedup student' >> beam.ParDo(DedupStudentRecordsFn())

    # write PCollection to log file
    distinct_student_pcoll | 'Write log 4' >> WriteToText(DIR_PATH + 'distinct_student_pcoll.txt')

    dataset_id = 'college_workflow_modeled'
    table_id = 'Student_Beam_DF'
    schema_id = 'sid:STRING,fname:STRING,lname:STRING,dob:DATE'

    # write PCollection to new BQ table
    distinct_student_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                table=table_id, 
                                                schema=schema_id,
                                                project=PROJECT_ID,
                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                batch_size=int(100))
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()