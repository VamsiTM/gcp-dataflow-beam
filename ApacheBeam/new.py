import apache_beam as beam
import argparse
import os
from apache_beam.options.pipeline_options import PipelineOptions

parser = argparse.ArgumentParser()
parser.add_argument('--input_file',help='The file path for the input text to process.')

args, beam_args = parser.parse_known_args()
beam_options = PipelineOptions(beam_args)    

serviceAccount = r"D:\5.LEARN\GCP\ApacheBeam\dataflow-course-373005-259cbd7173b1.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= serviceAccount

with beam.Pipeline(options=beam_options) as pipeline:

 Flight_Data = (
  pipeline#|"Read CSV File" >> beam.io.ReadFromText(args.input_file, skip_header_lines = 0)
  #| "Import Data time" >> beam.io.ReadFromText("gs://dataflow-course-beam/input/load_bq.csv")
   #| "Split by comma time" >> beam.Map(lambda record: record.split(','))
   | "Display" >> beam.Map(lambda x:x(beam_args.input_file))|beam.Map(print)
)


