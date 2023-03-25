import avro.schema
import fastavro
import tempfile
import os

from data_generator import generate_fake_data

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

        avro.schema.parse(open(f"{path}/schemas/inputs/user_generic.avsc", "rb").read()),
        avro.schema.parse(open(f"{path}/schemas/inputs/user_specific.avsc", "rb").read())
    ]
    for schema in schemas:

        # Generate fake data for the schema
        fake_data = generate_fake_data(schema)

        # Serialize the fake data to Avro binary format
        with tempfile.NamedTemporaryFile(delete=False) as f:
            fastavro.writer(f, schema.to_json(), [fake_data])
            f.flush()

            # Read the serialized Avro message back in and check that its schema matches the original schema
            f.seek(0)
            reader = fastavro.reader(f)
            assert reader.writer_schema == schema.to_json()

            # Check that the data in the serialized Avro message matches the generated fake data
            deserialized_data = next(reader)  # Get the first (and only) element from the reader object
            assert deserialized_data == fake_data