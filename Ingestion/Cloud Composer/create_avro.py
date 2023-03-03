import random
from faker import Faker
from avro.io import DatumWriter
from avro.datafile import DataFileWriter
from avro.schema import parse

def createFile():

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
    fake = Faker()

    with open("data.avro", "wb") as out_file:
        writer = DataFileWriter(out_file, DatumWriter(), schema)
        for i in range(20000):
            name = fake.name()
            age = random.randint(18, 65)
            email = fake.email()
            writer.append({"name": name, "age": age, "email": email})
        writer.close()

createFile()