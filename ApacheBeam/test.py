import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions



pipeline_options = {
    'project': 'dataflow-course-373005' ,
    'runner': 'DataflowRunner',
    'region': 'asia-south1',
    'staging_location': 'gs://dataflow-course-beam/temp',
    'temp_location': 'gs://dataflow-course-beam/temp',
    'template_location': 'gs://dataflow-course-beam/template/batch_job_df_bq_flights_test' ,
    'save_main_session': True 
    }

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

serviceAccount = r"D:\5.LEARN\GCP\ApacheBeam\dataflow-course-373005-259cbd7173b1.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= serviceAccount

table_schema = 'airport:STRING, list_Delayed_num:INTEGER, list_Delayed_time:INTEGER'
table = 'dataflow-course-373005:flights_dataflow.flights_aggr'

Flight_Data = (
p1| "Import Data time" >> beam.io.ReadFromText("gs://dataflow-course-beam/input/load_bq.csv")
  | "Split by comma time" >> beam.Map(lambda record: record.split(','))
  | "Write to BQ" >> beam.io.WriteToBigQuery(
                              table,
                              schema=table_schema,
                              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                              custom_gcs_temp_location = 'gs://dataflow-course-beam/temp' )
)

p1.run()