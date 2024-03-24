import apache_beam as beam
from google.cloud import bigquery
from apache_beam.runners.runner import PipelineState

client = bigquery.Client()
dataset_name = 'registrated_clients'
dataset_id = f'{client.project}.{dataset_name}'

try:
    client.get_dataset(dataset_id)
except:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset.description = "Registrated client details"
    dataset_ref = client.create_dataset(dataset, timeout=30)
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
        #exeption for date column
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

def to_json(row):
    cols = row.split(';')

    json_str = {'customer': cols[0],
                'birthday': cols[1],
                'nationality': cols[2],
                'sex': cols[3],
                'platform': cols[4]
                }
    return json_str

schema = 'customer:STRING, birthday:DATE, nationality:STRING, sex:STRING, platform:STRING'
pinterest_table_name = f'{client.project}:{dataset_name}.pinterest_data'
facebook_table_name = f'{client.project}:{dataset_name}.facebook_data'

p = beam.Pipeline()

cleaned_data = (
    p
    | beam.io.ReadFromText("customer_data.csv", skip_header_lines=True)
    | beam.Filter(is_empty)
    | beam.Distinct()
    | beam.Map(name_capitalize)
    | beam.Map(symbols_cleaning)
    | beam.Map(join_names)
    | beam.Map(date_unify)
)

pinterest_data = (
    cleaned_data
    | beam.Filter(lambda row: row.split(';')[4].lower() == 'pinterest')
)

facebook_data = (
    cleaned_data
    | beam.Filter(lambda row: row.split(';')[4].lower() == 'facebook')
)

(
    cleaned_data
    | 'total count' >> beam.combiners.Count.Globally()
    | 'total map' >> beam.Map(lambda x: 'Total Count: '+ str(x))
    | 'total print' >> beam.Map(print)
)

(
    pinterest_data
    | 'pinterest count' >> beam.combiners.Count.Globally()
    | 'pinterest map' >> beam.Map(lambda x: 'Pinterest count: ' + str(x))
    | 'pinterest print' >> beam.Map(print)
)

(
    facebook_data
    | 'facebook count' >> beam.combiners.Count.Globally()
    | 'facebook map' >> beam.Map(lambda x: 'Facebook count: ' + str(x))
    | 'facebook print' >> beam.Map(print)
)

(
    pinterest_data
    | 'pinterest to json' >> beam.Map(to_json)
    | 'write pinterest' >> beam.io.WriteToBigQuery(
        table=pinterest_table_name,
        schema=schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        custom_gcs_temp_location='gs://temp_beam_location',
        additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
        #jakaś data wrzucenia by się przydała!!! i id procesu!!
    )
)

(
    facebook_data
    | 'facebook to json' >> beam.Map(to_json)
    | 'write facebook' >> beam.io.WriteToBigQuery(
        table=facebook_table_name,
        schema=schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        custom_gcs_temp_location='gs://temp_beam_location',
        additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
        #jakaś data wrzucenia by się przydała!!! i id procesu!!
    )
)

pipeline_state = p.run().wait_until_finish()

if pipeline_state == PipelineState.DONE:
    print('Pipeline has completed successfully.')
elif pipeline_state == PipelineState.FAILED:
    print('Pipeline has failed.')
else:
    print('Pipeline in unknown state.')
