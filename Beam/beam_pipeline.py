import apache_beam as beam

def is_empty(row):
    cols = row.split(';')
    if cols[0] == '' or cols[2] == '' or cols[3] == '' or cols[4] == '' :
        return False
    else:
        return True

def name_capitalize(row):
    cols = row.split(';')
    cols[0] = cols[0].capitalize()
    return ';'.join(cols)

def surname_cleaning(row):
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

def date_unify(row):
    import pandas as pd
    cols = row.split(';')
    cols[3] = str(pd.to_datetime(cols[3], dayfirst=True).date())
    return ';'.join(cols)


p = beam.Pipeline()

(p
 | beam.io.ReadFromText("names_data.csv", skip_header_lines=True)
 | beam.Filter(is_empty)
 | beam.Map(name_capitalize)
 | beam.Map(surname_cleaning)
 | beam.Map(date_unify)
 | beam.Map(print)
)

p.run()

#first install apache_beam !pip install apache_beam & pandas