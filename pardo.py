import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs on each element in the input PCollection.
class CreateDateFn(beam.DoFn):
  def process(self, element):
    record = element
    house = record.get('Household_ID')
    year = record.get('Survey_Year')
    month_raw = record.get('Interview_Month')
    month = month_raw.split(' ')[0]
    date = str(year)+'-'+month+'-01'
    return [(house,date)]

    
# DoFn performs on each element in the input PCollection.
class MakeRecordFn(beam.DoFn):
  def process(self, element):
     house_id, date = element
     record = {'house_id': house_id, 'date': date}
     return [record] 

PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM heath_interview_split.household'))

    # write PCollection to input.txt file
    query_results | 'Write to input.txt' >> WriteToText('input.txt')

    # apply a ParDo to the PCollection 
    date_pcoll = query_results | 'Create Date' >> beam.ParDo(CreateDateFn())

    # write PCollection to a file
    date_pcoll | 'Write File' >> WriteToText('output.txt')
    
    # make BQ records
    out_pcoll = date_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
    
    qualified_table_name = PROJECT_ID + ':heath_interview_split.household_date'
    table_schema = 'house_id:INTEGER,date:DATE'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
