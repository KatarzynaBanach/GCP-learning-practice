# using funtion to transform pipeline
import apache_beam as beam

# function
def number_add(pcall,el):
  return (
      pcall
      | beam.Map(lambda x: x+el)
  )
  
p = beam.Pipeline()


number = p | beam.Create([1,2,3,4,5])

# passing pipeline as a function
final_p = number_add(number,1)

# print pcollection
final_p | beam.Map(print)
p.run()
