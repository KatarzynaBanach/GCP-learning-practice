import apache_beam as beam
from google.cloud import bigquery
import google.cloud.storage as gcs
from apache_beam.runners.runner import PipelineState
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
import google.cloud.storage as gcs
import os
import yaml

gcs_bucket = 'temp_beam_location_1'

def load_from_yaml(file_path):
  with open(file_path, 'r') as f:
    config = yaml.safe_load(f)
  return config
  
def get_arguments():
  parser = argparse.ArgumentParser()

  parser.add_argument('--project', required=True, help='GCP project')
  parser.add_argument('--region', required=True, help='GCP region')
  group = parser.add_mutually_exclusive_group()
  group.add_argument('--DirectRunner', action='store_true')
  group.add_argument('--DataflowRunner', action='store_true')

  args = parser.parse_args()

  runner='DirectRunner'
  if args.DataflowRunner:
      runner='DataflowRunner'

  return dict(project=args.project,
              region=args.region,
              runner=runner)

def name_capitalize(row):
    cols = row.split(';')
    cols[0] = cols[0].capitalize()
    return ';'.join(cols)

def symbols_cleaning(row):
    import re
    cols = row.split(';')
    row = []
    for idx, col in enumerate(cols):
        # exeption for date column
        if idx == 3:
            row.append(col)
        else:
            row.append(re.sub(r'[#$%&.?]','',col))

    return ';'.join(row)

def join_names(row):
    cols = row.split(';')
    names = ' '.join([c for c in cols[0:3] if c != ''])
    rest = ';'.join(cols[3:])
    return names + ';' + rest

def date_unify(row):
    import pandas as pd
    cols = row.split(';')
    cols[1] = str(pd.to_datetime(cols[1], dayfirst=False).date())
    return ';'.join(cols)

def add_created_at_and_id(row):
    from datetime import datetime
    _created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return f'{row};{_created_at}'

def delete_platform_column(row):
    cols = row.split(';')
    del cols[4]
    return ';'.join(cols)

def to_json(row):
    cols = row.split(';')

    json_str = {'customer': cols[0],
                'birthday': cols[1],
                'nationality': cols[2],
                'sex': cols[3],
                '_created_at': cols[4]
                }
    return json_str

def split_by_sources(pcol, source, sources_list):
    if source in sources_list:
      return ( pcol
              | f'{source} filter' >> beam.Filter(lambda row: row.split(';')[4].lower() == source)
              | f'{source} delete col' >> beam.Map(delete_platform_column)
              )
    else:
      return ( pcol
              | f'{source} filter' >> beam.Filter(lambda row: row.split(';')[4].lower() not in sources_list)    # list of sources
              | f'{source} delete col' >> beam.Map(delete_platform_column)
              )
      

def write_to_bq(pcol, source, table_name, schema_definition):
    return ( pcol
            | f'{source} to json' >> beam.Map(to_json)
            | f'{source} write' >> beam.io.WriteToBigQuery(
                table=table_name,
                schema=schema_definition,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                custom_gcs_temp_location='gs://temp_beam_location_1/staging',
                additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
                )
            )


def row_count_write(pcol, source):
    output_results = f'gs://temp_beam_location_1/easy_beam_test/count_{source.lower()}'
    return ( pcol
            | f'{source} count' >> beam.combiners.Count.Globally()
            | f'{source} map' >> beam.Map(lambda x: f'{source} count: '+ str(x))
            | f'{source} to_cs' >> beam.io.textio.WriteToText(output_results,file_name_suffix='.txt')
            )

def run():
  
# Loading configuration from yaml.
  config = load_from_yaml('config.yaml')
  sources_list = config['sources']
  schema_definition = ', '.join([f"{key}:{value}" for key, value in config['schema_definition'].items()])

  # Create dataset if needed.
  client = bigquery.Client()
  dataset_name = 'registrated_clients'
  dataset_id = f'{client.project}.{dataset_name}'

  # Get names for tables based on current project.
  pinterest_table_name = f'{client.project}:{dataset_name}.pinterest_data'
  facebook_table_name = f'{client.project}:{dataset_name}.facebook_data'
  
  try:
      client.get_dataset(dataset_id)
  except:
      dataset = bigquery.Dataset(dataset_id)
      dataset.location = "EU"
      dataset.description = "Registrated client details"
      dataset_ref = client.create_dataset(dataset, timeout=60)
  def is_empty(row):
      cols = row.split(';')
      if cols[0] == '' or cols[2] == '' or cols[3] == '' or cols[4] == '' or cols[6] == '' :
          return False
      else:
          return True
  
  # Clear bucket with results.
  bucket = gcs.Client().get_bucket(gcs_bucket)
  for blob in bucket.list_blobs(prefix='easy_beam_test/'):
    blob.delete()
  
  # argv = [
  # '--project={0}'.format(client.project),
  # '--job_name=myjob',
  # '--save_main_session',
  # '--worker_machine_type=e2-standard-2',
  # '--staging_location=gs://{0}/staging/'.format("gs://temp_beam_location/staging"),
  # '--temp_location=gs://{0}/staging/'.format("gs://temp_beam_location/temp"),
  # '--runner={0}'.format("DataflowRunner"),
  # '--region={0}'.format("us-central1"),
  # '--max_num_workers=5'
  # ]
  # p = beam.Pipeline(argv=argv)
  
  
  # Get and pass options for Pipeline.
  args = get_arguments()
  options = PipelineOptions(
      project=args['project'],
      region=args['region'],  # Choose the appropriate region
      job_name='examplejob4',
      temp_location='gs://temp_beam_location_1/staging', 
      staging_location='gs://temp_beam_location_1/staging',
      runner=args['runner'],
      worker_machine_type='e2-standard-2'
  )
  
  p = beam.Pipeline(options=options)
  
  # Reading data from Cloud Storage and cleaning it.
  cleaned_data = (
      p
      | beam.io.ReadFromText("gs://temp_beam_location_1/customer/customer_data.csv", skip_header_lines=True)
      | beam.Filter(is_empty)
      | beam.Distinct()
      | beam.Map(name_capitalize)
      | beam.Map(symbols_cleaning)
      | beam.Map(join_names)
      | beam.Map(date_unify)
      | beam.Map(add_created_at_and_id)
  )
  
  # Splitting pcollections by source.
  pinterest_data = split_by_sources(cleaned_data, 'pinterest', sources_list)
  facebook_data = split_by_sources(cleaned_data, 'facebook', sources_list)
  other_data = split_by_sources(cleaned_data, 'other', sources_list)

  # Counting rows and writing the information into Cloud Storage.
  row_count_write(cleaned_data, 'Total')
  row_count_write(pinterest_data, 'Pinterest')
  row_count_write(facebook_data, 'Facebook')
  row_count_write(other_data, 'other')

  # Writing splitted data into BigQuery.
  write_to_bq(pinterest_data, 'pinterest', pinterest_table_name, schema_definition)
  write_to_bq(facebook_data, 'facebook', facebook_table_name, schema_definition)
  
  # Runing pipeline.
  pipeline_state = p.run().wait_until_finish()
  
  # Getting state of pipeline and writing it into Cloud Storage.
  pipeline_result = ''
  if pipeline_state == PipelineState.DONE:
      pipeline_result = 'Pipeline has completed successfully.'
  elif pipeline_state == PipelineState.FAILED:
      pipeline_result = 'Pipeline has failed.'
  else:
      pipeline_result = 'Pipeline in unknown state.'
  
  with open('pipeline_status.txt', 'w') as f:
      f.write(pipeline_result)
  bucket.blob('easy_beam_test/pipeline_status.txt').upload_from_filename('pipeline_status.txt')
  if os.path.exists("pipeline_status.txt"):
    os.remove("pipeline_status.txt")

if __name__ == '__main__':
  run()
