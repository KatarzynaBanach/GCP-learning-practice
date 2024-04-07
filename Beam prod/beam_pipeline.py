# ok
import apache_beam as beam
from google.cloud import bigquery
import google.cloud.storage as gcs
from apache_beam.runners.runner import PipelineState

from apache_beam.options.pipeline_options import PipelineOptions

client = bigquery.Client()
dataset_name = 'registrated_clients'
dataset_id = f'{client.project}.{dataset_name}'

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


schema_definition = 'customer:STRING, birthday:DATE, nationality:STRING, sex:STRING, _created_at:DATETIME'

pinterest_table_name = f'{client.project}:{dataset_name}.pinterest_data'
facebook_table_name = f'{client.project}:{dataset_name}.facebook_data'


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

options = PipelineOptions(
    project='digital-bonfire-419015',
    region='europe-central2',  # Choose the appropriate region
    job_name='examplejob2',
    temp_location='gs://temp_beam_location_1/staging', 
    staging_location='gs://temp_beam_location_1/staging',
    runner='DataflowRunner',
    worker_machine_type='e2-standard-2'
)

# -------

p = beam.Pipeline(options=options)


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

pinterest_data = (
    cleaned_data
    | 'pinterest filter' >> beam.Filter(lambda row: row.split(';')[4].lower() == 'pinterest')
    | 'pinterest delete col' >> beam.Map(delete_platform_column)
)

facebook_data = (
    cleaned_data
    | 'facebook filter' >> beam.Filter(lambda row: row.split(';')[4].lower() == 'facebook')
    | 'facebook delete col' >> beam.Map(delete_platform_column)
)

# clean_result = (
#     cleaned_data
#     | 'total count' >> beam.combiners.Count.Globally()
#     | 'total map' >> beam.Map(lambda x: 'Total Count: '+ str(x))
# )

# pinterest_result = (
#     pinterest_data
#     | 'pinterest count' >> beam.combiners.Count.Globally()
#     | 'pinterest map' >> beam.Map(lambda x: 'Pinterest count: ' + str(x))
# )

# facebook_result =(
#     facebook_data
#     | 'facebook count' >> beam.combiners.Count.Globally()
#     | 'facebook map' >> beam.Map(lambda x: 'Facebook count: ' + str(x))
# )

(
    pinterest_data
    | 'pinterest to json' >> beam.Map(to_json)
    | 'write pinterest' >> beam.io.WriteToBigQuery(
        table=pinterest_table_name,
        schema=schema_definition,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        custom_gcs_temp_location='gs://temp_beam_location_1/staging',
        additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
    )
)

(
    facebook_data
    | 'facebook to json' >> beam.Map(to_json)
    | 'write facebook' >> beam.io.WriteToBigQuery(
        table=facebook_table_name,
        schema=schema_definition,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        custom_gcs_temp_location='gs://temp_beam_location_1/staging',
        additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
    )
)

pipeline_state = p.run().wait_until_finish()

pipeline_result = ''
if pipeline_state == PipelineState.DONE:
    pipeline_result = 'Pipeline has completed successfully.'
elif pipeline_state == PipelineState.FAILED:
    pipeline_result = 'Pipeline has failed.'
else:
    pipeline_result = 'Pipeline in unknown state.'


import google.cloud.storage as gcs
import os

with open('pipeline_status.txt', 'w') as f:
    f.write(pipeline_result)

gcs_bucket = 'temp_beam_location_1'
bucket = gcs.Client().get_bucket(gcs_bucket)

for blob in bucket.list_blobs(prefix='pipeline_result/'):
  blob.delete()

bucket.blob('pipeline_result/pipeline_status.txt').upload_from_filename('pipeline_status.txt')
if os.path.exists("pipeline_status.txt"):
  os.remove("pipeline_status.txt")