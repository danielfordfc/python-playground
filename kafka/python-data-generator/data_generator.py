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
    """
    Generates fake data in a Python dictionary that conforms to the given Avro schema.
    """
    fake_data = {}
    if isinstance(schema, RecordSchema):
        for field in schema.fields:
            field_name = field.name
            field_type = field.type
            qualified_field_name = field_name
            # Check if the namespace is present in the schema and prepend it to the name
            #if schema.namespace and isinstance(field_type, RecordSchema):
            if schema.namespace and isinstance(field, RecordSchema):
                # think this is correct?
                field_name = f"{schema.namespace}.{field_name}"
            # Use qualified_field_name for RecordSchema fields and field_name for other fields
            if isinstance(field_type, ArraySchema):
                if isinstance(field_type.items, RecordSchema):
                    fake_data[field_name] = [dict(generate_fake_data(field_type.items))]
                elif isinstance(field_type.items, ArraySchema):
                    # Recursively call the function for nested arrays
                    fake_data[field_name] = [generate_fake_data(field_type.items)]
                else:
                    fake_data[field_name] = [generate_fake_data(field_type.items)]
            elif isinstance(field_type, RecordSchema):
                fake_data[qualified_field_name] = dict(generate_fake_data(field_type))
            elif isinstance(field_type, MapSchema):
                if isinstance(field_type.values, RecordSchema):
                    fake_data[field_name] = {f'key{i}': generate_fake_data(field_type.values) for i in range(3)}
                elif isinstance(field_type.values, ArraySchema):
                    fake_data[field_name] = {f'key{i}': [generate_fake_data(field_type.values.items) for _ in range(3)]
                                             for i in range(3)}
                else:
                    fake_data[field_name] = {f'key{i}': process_primitive_type(field_type.values) for i in range(3)}
            elif isinstance(field_type, PrimitiveSchema):
                fake_data[field_name] = process_primitive_type(field_type)
            elif isinstance(field_type, EnumSchema):
                fake_data[field_name] = random.choice(field_type.symbols)
            elif isinstance(field_type, UnionSchema):
                non_null_types = [t for t in field_type.schemas if t.type != 'null']
                rnd_type = random.choice(non_null_types)
                if isinstance(rnd_type, PrimitiveSchema):
                    fake_data[field_name] = process_primitive_type(rnd_type)
                elif isinstance(rnd_type, ArraySchema):
                    if isinstance(rnd_type.items, RecordSchema):
                        fake_data[field_name] = [generate_fake_data(rnd_type.items) for _ in range(3)]
                    elif isinstance(rnd_type.items, EnumSchema):
                        fake_data[field_name] = [random.choice(rnd_type.items.symbols) for _ in range(3)]
                    elif isinstance(rnd_type.items, MapSchema):
                        if isinstance(rnd_type.items.values, RecordSchema):
                            fake_data[field_name] = {f'key{i}': generate_fake_data(rnd_type.items.values) for i in range(3)}
                        else:
                            fake_data[field_name] = {f'key{i}': process_primitive_type(rnd_type.items.values) for i in
                                                     range(3)}
                elif isinstance(rnd_type, MapSchema):
                    if isinstance(rnd_type.values, RecordSchema):
                        fake_data[field_name] = {f'key{i}': generate_fake_data(rnd_type.values) for i in range(3)}
                    else:
                        fake_data[field_name] = {f'key{i}': process_primitive_type(rnd_type.values) for i in
                                                 range(3)}
                elif isinstance(rnd_type, RecordSchema):
                    fake_data[field_name] = generate_fake_data(rnd_type)
                elif isinstance(rnd_type, EnumSchema):
                    fake_data[field_name] = random.choice(rnd_type.symbols)
                else:
                    fake_data[field_name] = generate_fake_data(field_type)

            else:
                raise ValueError
    return fake_data


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
    schema_parse = avro.schema.parse(open(f"{path}/schemas/inputs/fca-loan-drawdowns.avsc", "rb").read())

    data_payload = generate_fake_data(schema_parse)

    with open(f"{path}/schemas/outputs/khan.json", "w") as f:
        f.write(json.dumps(data_payload, indent=4))
