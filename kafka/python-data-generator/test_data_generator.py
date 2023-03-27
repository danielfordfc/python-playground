import avro.schema
import fastavro
import tempfile
import os

from data_generator import generate_fake_data
import json
"""
I would like to write a test suite to test different schemas, whereby I'll take a schema,
run the function on it to generate the payload, 
then try to serialize that payload back into the avro schema and confirm its accepted
"""

def test_generate_fake_data():
    # Define the Avro schemas to test
    path = os.path.dirname(os.path.abspath(__file__))

    schemas = [
        # tests passing when schema doesn't contain a namespace.
        # needs to be handled in the code to output fully qualified names on data generation...
        # i.e. User field with a namespace of  "namespace": "confluent.io.examples.serialization.avro", "name": "User",
        # would need to resolve to 'name': 'confluent.io.examples.serialization.avro.User',

        #avro.schema.parse(open(f"{path}/schemas/inputs/user_generic.avsc", "rb").read()),
        #avro.schema.parse(open(f"{path}/schemas/inputs/user_specific.avsc", "rb").read()),
        avro.schema.parse(open(f"{path}/schemas/inputs/fca-afmas.json", "rb").read())
    ]
    for schema in schemas:

        # Generate fake data for the schema
        fake_data = generate_fake_data(schema)

        # Serialize the fake data to Avro binary format
        with tempfile.NamedTemporaryFile(delete=False) as f:
            fastavro.writer(f, schema.to_json(), [fake_data])
            # its basically whinging that there's no data with name 'name' when trying to coerce the json into the sch,
            # because only 'confluent.io.examples.serialization.avro.name' exists...
            f.flush()

            # Read the serialized Avro message back in and check that its schema matches the original schema
            f.seek(0)
            reader = fastavro.reader(f)

            # when namespaces arent handled in the fake_data_generator, then schema.to_json() contains namespace field,
            # that isn't present in the writer_schema. to_canonical_json seems to work though
            assert reader.writer_schema == json.loads(json.dumps(schema.to_canonical_json()))
            #assert reader.writer_schema == schema.to_json()

            # Check that the data in the serialized Avro message matches the generated fake data
            deserialized_data = next(reader)

            assert deserialized_data == fake_data
