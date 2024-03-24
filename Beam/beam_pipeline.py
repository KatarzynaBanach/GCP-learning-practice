import apache_beam as beam

def name_capitalize(row):
    cols = row.split(';')
    cols[0] = cols[0].capitalize()
    return ';'.join(cols)

def surname_cleaning(row):
    import re
    cols = row.split(';')
    cols[2] = 

p = beam.Pipeline()

(p
 | beam.io.ReadFromText("names_data.csv", skip_header_lines=True)
 | beam.Map(name_capitalize)
 | beam.Map(print)
)

p.run()

#first install apache_beam !pip install apache_beam