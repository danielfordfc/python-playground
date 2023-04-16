import os
import avro.schema

from avro.schema import *
from faker import Faker
import uuid
import random

import datetime as dt
import time
import string
import glob
import base64


"""
Functionality TODOs:
@TODO: Handle all Logical Types inside PrimitiveSchema parsing
@TODO: Write Unit Tests for every single edge case and common schema structures
    @TODO: Confirm Parity with actual record payloads seen for these topics
    @TODO: I believe in Unions of type Records, its returning both possible records, instead of just one, under the parent key
@TODO: Handle namespaces properly

@DONE: Handle MapTypes
@DONE: Handle OneOf UnionTypes by randomly selecting the element to process.
"""

"""
QoL TODOs:
@TODO: Use `Faker` instead of `random`
@TODO: Have the possibility of declaring the default:
@TODO: Can we have a config file that declares limits for the random values?
    @TODO: Add an iterator that can be used to track the number of records produced
    (i.e. if we take a random string in the schema, we can use this to track the number of records
        produced and use it as a PK
@TODO: Produce a README.md file that explains how to use this
@TODO: Handle document types!?
"""


#Make this configurable

ARRAY_MIN_LENGTH = 1
ARRAY_MIN_LENGTH_DEFAULT = 1
ARRAY_MAX_LENGTH = 2
ARRAY_MAX_LENGTH_DEFAULT = 10
INT_MIN = 100
INT_MAX = 500
INT_MIN_DEFAULT = 0
INT_MAX_DEFAULT = 999


def generate_fake_data(schema):
    if isinstance(schema, RecordSchema):
        return generate_record_data(schema)
    elif isinstance(schema, ArraySchema):
        return generate_array_data(schema)
    elif isinstance(schema, MapSchema):
        return generate_map_data(schema)
    elif isinstance(schema, PrimitiveSchema):
        return process_primitive_type(schema)
    elif isinstance(schema, EnumSchema):
        return random.choice(schema.symbols)
    elif isinstance(schema, UnionSchema):
        return generate_union_data(schema)
    elif isinstance(schema, FixedSchema):
        return generate_fixed_data(schema)
    else:
        raise ValueError(f"Unsupported schema type: {schema}")


def generate_record_data(schema):
    data = {}
    for field in schema.fields:
        field_name = field.name
        field_type = field.type

        if schema.namespace and isinstance(field_type, RecordSchema):
            field_name = f"{schema.namespace}.{field_name}"

        data[field_name] = generate_fake_data(field_type)

    return data


def generate_array_data(schema):
    """
    @TODO: Make the min and max length configurable
    """

    items_schema = schema.items

    return [generate_fake_data(items_schema) for _ in range(random.randint(ARRAY_MIN_LENGTH, ARRAY_MAX_LENGTH))]


def generate_map_data(schema):
    values_schema = schema.values
    map_size = 3  # Consider making this configurable

    return {f'key{i}': generate_fake_data(values_schema) for i in range(map_size)}


def generate_union_data(schema):
    non_null_types = [t for t in schema.schemas if t.type != 'null']
    rnd_type = random.choice(non_null_types)

    return generate_fake_data(rnd_type)


def generate_fixed_data(schema):
    size = schema.size
    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=size))
    random_data = bytearray(random_string, 'utf-8')
    return base64.b64encode(random_data).decode('utf-8')

def get_min_max_values(field_type):
    default_min_max = {
        "int": {"min": 0, "max": 2**31 - 1},
        "long": {"min": 0, "max": 2**63 - 1},
        "float": {"min": 0, "max": 3.4e+38},
        "double": {"min": 0, "max": 1.8e+308},
        "string": {"min": 1, "max": 20}
    }

    # Determine the type of primitive type we're working with
    data_type = field_type.type

    # Check if the min_value attribute exists in the field_type object
    min_value = field_type.min_value if hasattr(field_type, "min_value") else default_min_max[data_type]["min"]
    max_value = field_type.max_value if hasattr(field_type, "max_value") else default_min_max[data_type]["max"]

    return min_value, max_value


def process_primitive_type(field_type):
    """
    @TODO: Properly Handle Logical Types
    @TODO: Properly allow setting ranges for random values for all primitive types AND logical types
    """

    permitted_min_maxes = ['int','long','float','double','string']
    if field_type.type in permitted_min_maxes:
        min_value, max_value = get_min_max_values(field_type)

    # Handle primitive fields
    logical_type = field_type.other_props.get('logicalType')
    if field_type.type == 'string' and logical_type == 'uuid':
        return str(uuid.uuid4())
    if field_type.type == 'int':
        return random.randint(min_value, max_value)
    elif field_type.type == 'long':
        if 'logicalType' in field_type.other_props and field_type.other_props['logicalType'] == 'timestamp-millis':
            return int(time.time() * 1000)
        else:
            return random.randint(min_value, max_value)
    elif field_type.type == 'float':
        return random.uniform(min_value, max_value)
    elif field_type.type == 'double':
        return random.uniform(min_value, max_value)
    elif field_type.type == 'string':
        return ''.join(random.choices(['a', 'b', 'c', 'd', 'e', 'f'], k=10))
    elif field_type.type == 'boolean':
        return random.choice([True, False])
    else:
        raise ValueError(f"Unsupported field type: {field_type.type}")


if __name__ == "__main__":
    # Define the path to the Avro schemas
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schemas/inputs")

    # Load all the Avro schemas in the "inputs" folder
    schema_files = glob.glob(os.path.join(path, "*.avsc"))

    for schema in schema_files:

        filename = os.path.basename(schema).replace(".avsc", ".json")

        schema_parse = avro.schema.parse(open(schema, "rb").read())

        print(f'attempting to generate fake data for schema: {filename}')
        data_payload = generate_fake_data(schema_parse)

        # get the path to the outputs folder
        output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schemas/outputs")

        with open(f"{output_path}/{filename}", "w") as f:
            f.write(json.dumps(data_payload, indent=4))
            print(f'Wrote file: {filename}')
        f.close()
