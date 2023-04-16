import os
import avro.schema

from avro.schema import *
from faker import Faker
import uuid
import random

import datetime as dt
import time


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
    items_schema = schema.items
    array_length = 3  # Consider making this configurable

    return [generate_fake_data(items_schema) for _ in range(array_length)]


def generate_map_data(schema):
    values_schema = schema.values
    map_size = 3  # Consider making this configurable

    return {f'key{i}': generate_fake_data(values_schema) for i in range(map_size)}


def generate_union_data(schema):
    non_null_types = [t for t in schema.schemas if t.type != 'null']
    rnd_type = random.choice(non_null_types)

    return generate_fake_data(rnd_type)


def process_primitive_type(field_type):
    # Handle primitive fields
    logical_type = field_type.other_props.get('logicalType')

    if field_type.type == 'string' and logical_type == 'uuid':
        return uuid.uuid4()

    if field_type.type == 'int':
        # if 'logicalType' in field_type.other_props and field_type.other_props['logicalType'] == 'date':
        #     fake_data[field_name] = datetime.date(random.randint(1970, 2022), random.randint(1, 12), random.randint(1, 28)).strftime("%Y-%m-%d")
        # else:
        return random.randint(0, 100)
    elif field_type.type == 'long':
        if 'logicalType' in field_type.other_props and field_type.other_props['logicalType'] == 'timestamp-millis':
            return int(time.time() * 1000)
        else:
            return random.randint(0, 1000000)
    elif field_type.type == 'float':
        return random.uniform(0, 100)
    elif field_type.type == 'double':
        return random.uniform(0, 1000000)
    elif field_type.type == 'string':
        return ''.join(random.choices(['a', 'b', 'c', 'd', 'e', 'f'], k=10))
    elif field_type.type == 'boolean':
        return random.choice([True, False])
    else:
        raise ValueError(f"Unsupported field type: {field_type.type}")


if __name__ == "__main__":
    path = os.path.realpath(os.path.dirname(__file__))
    schema_parse = avro.schema.parse(open(f"{path}/schemas/inputs/fca-6.avsc", "rb").read())

    data_payload = generate_fake_data(schema_parse)

    with open(f"{path}/schemas/outputs/fca-6-payload.json", "w") as f:
        f.write(json.dumps(data_payload, indent=4))
