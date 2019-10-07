import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# PTransform: parse line in file, return (actor name, 1)
class ActorCountFn(beam.DoFn):
  def process(self, element):
    values = element.strip().split('\t')
    year = values[0]
    category = values[1]
    winner = values[2]
    entity = values[3]

    if 'ACTOR' in category or 'ACTRESS' in category:
	    return [(entity, 1)]    

# PTransform: sum up nominations for a given actor/actress
class ActorSumFn(beam.DoFn):
  def process(self, element):
     actor, counts = element
     total_count = len(counts)
     return [(actor, total_count)]  
    
# Create a Pipeline using a local runner for execution
with beam.Pipeline('DirectRunner') as p:

    # create a PCollection from the file contents.
    in_pcoll = p | 'Read File' >> ReadFromText('oscars_input.tsv')

    # apply ParDo to the PCollection 
    actor_pcoll = in_pcoll | 'Extract Actor' >> beam.ParDo(ActorCountFn())

    # apply GroupByKey to the PCollection
    group_pcoll = actor_pcoll | 'Group by Actor' >> beam.GroupByKey()

    # apply ParDo to the PCollection
    out_pcoll = group_pcoll | 'Sum up Counts' >> beam.ParDo(ActorSumFn())

    # write PCollection to a file
    out_pcoll | 'Write File' >> WriteToText('oscars_output.txt')
