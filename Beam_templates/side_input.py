import apache_beam as beam

def multiply_by_side_input(element, side_input):
    side_value = side_input['multiplier']
    return element * side_value

with beam.Pipeline() as p:
    # Main input PCollection
    main_input = p | beam.Create([1, 2, 3, 4, 5])

    # Side input PCollection
    side_input = p | "Create side input" >> beam.Create([{'multiplier': 10}])

    # Apply beam.Map with side input
    multiplied_values = main_input | beam.Map(multiply_by_side_input, side_input=beam.pvalue.AsSingleton(side_input))

    # Output
    multiplied_values | beam.Map(print)
#  AsSingleton(): A function used to convert a PCollectionView into a single value. 
# This is commonly used when you have a side input PCollectionView containing a single value, such as a configuration or lookup value.
