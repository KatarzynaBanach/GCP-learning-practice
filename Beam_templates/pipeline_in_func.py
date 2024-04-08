# using funtion to transform pipeline
import apache_beam as beam

def number_add(pcall,el):
  return (
      pcall
      | beam.Map(lambda x: x+el)
  )
  
p = beam.Pipeline()

number = p | beam.Create([1,2,3,4,5])
final_p = number_add(number,1)  # passing pipeline as a function

final_p | beam.Map(print)
p.run()



# --------------------------------------------------------------------------------------------
# it is nice option to make ptransformations resable

def number_add(pcall,el):
  return (
      pcall
      | f'add {el}' >> beam.Map(lambda x: x+el)
  )

p = beam.Pipeline()
numbers = p | beam.Create([1,2,3,4,5])

final_p = number_add(numbers,1)
final_p | 'print_1' >> beam.Map(print)

final_2 = number_add(numbers,10)
final_2 | 'print_10' >> beam.Map(print)

p.run()
