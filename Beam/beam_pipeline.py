import apache_beam as beam

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


p = beam.Pipeline()

cleaned_data = (
    p
    | beam.io.ReadFromText("names_data.csv", skip_header_lines=True)
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

p.run()

#first install apache_beam !pip install apache_beam & pandas

#ideas: country mapping from different file
#duplicated check!
#new platform check
#coś z tym Flat map by się też przydało
#beam from csv?