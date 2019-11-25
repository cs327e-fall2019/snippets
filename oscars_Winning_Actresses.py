import os, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class ParseNameFn(beam.DoFn):
  def process(self, element):
    record = element
    name = record.get('name')
      
    split_name = name.split(' ')
    if len(split_name) > 1:
        fname = split_name[0]
        lname = split_name[1]
    else:
        fname = split_name[0]
        lname = 'None'
    
    record.pop('name', None)
    record.update({'fname' : fname})
    record.update({'lname' : lname})
    
    return [record]
           
def run():
    
    PROJECT_ID = 'cs327e-fa2019'

    # Project ID is required when using the BQ source
    options = {
        'project': PROJECT_ID
    }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    # Create beam pipeline using local runner
    p = beam.Pipeline('DirectRunner', options=opts)

    sql = 'SELECT name, year, category FROM oscars.Winning_Actresses'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # apply ParDo to parse the actress's name 
    parsed_pcoll = query_results | 'Parse Name' >> beam.ParDo(ParseNameFn())

    dataset_id = 'oscars'
    table_id = 'Winning_Actresses_Beam'
    schema_id = 'fname:STRING,lname:STRING,year:INTEGER,category:STRING'

    # write PCollection to new BQ table
    parsed_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                table=table_id, 
                                                schema=schema_id,
                                                project=PROJECT_ID,
                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                batch_size=int(500))
    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()