import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions(
    # runner='DirectRunner'
    project='digital-bonfire-419015',
    region='europe-central2',  # Choose the appropriate region
    job_name='examplejob2',
    save_main_session='true',
    temp_location='gs://temp_beam_location_1/staging/',
    staging_location='gs://temp_beam_location_1/staging/',
    runner='DataflowRunner',
    worker_machine_type='e2-standard-2'
)

output_results = 'gs://temp_beam_location_1/easy_beam_test/numbers'

p = beam.Pipeline(options=options)
( p 
  | beam.Create([1, 2, 3, 4, 5])
  | beam.Map(lambda x: x+1)
  | beam.io.textio.WriteToText(output_results,file_name_suffix='.csv'))
p.run()
