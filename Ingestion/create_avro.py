import random
from avro.io import DatumWriter
from avro.datafile import DataFileWriter
from avro.schema import parse

schema_str = """
{
  "type": "record",
  "name": "Person",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"},
    {"name": "email", "type": "string"}
  ]
}
"""

schema = parse(schema_str)

with open("data.avro", "wb", encoding="utf-8") as out_file:
    writer = DataFileWriter(out_file, DatumWriter(), schema)
    for i in range(20000):
        name = f"Person {i}"
        age = random.randint(18, 65)
        email = f"person{i}@example.com"
        writer.append({"name": name, "age": age, "email": email})
    writer.close()
