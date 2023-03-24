import os
import avro.schema

from avro.schema import *
from faker import Faker
import uuid
import random

"""
@TODO: Handle all Logical Types inside PrimitiveSchema parsing
@DONE: Handle MapTypes
@DONE: Handle OneOf UnionTypes by randomly selecting the element to process.
@TODO: Confirm Parity with actual record payloads seen for these topics
    @TODO: I believe in Unions of type Records, its returning both possible records, instead of just one, under the parent key
@TODO: Write Unit Tests for all of this
@TODO: Use `Faker` instead of `random`
@TODO: Have the possibility of declaring the default:
@TODO: Have the possibility of the union declaring a null (this may be the same as declaring the default)
"""

def generate_fake_data(schema):
    """
    Generates fake data in a Python dictionary that conforms to the given Avro schema.
    """
    fake_data = {}
    if isinstance(schema, UnionSchema):
        # Handle union fields
        for field_type in schema.schemas:
            if field_type.type != 'null':
                return generate_fake_data(field_type)
        return None

    elif isinstance(schema, RecordSchema):
        for field in schema.fields:
            field_name = field.name
            field_type = field.type
            if isinstance(field_type, ArraySchema):
                # Handle array fields
                if isinstance(field_type.items, RecordSchema):
                    fake_data[field_name] = [generate_fake_data(field_type.items)]
                else:
                    fake_data[field_name] = [generate_fake_data(field_type.items) for _ in range(3)]
            elif isinstance(field_type, RecordSchema):
                # Handle record fields
                fake_data[field_name] = generate_fake_data(field_type)
            elif isinstance(field_type, MapSchema):
                # Handle map fields
                if isinstance(field_type.values, RecordSchema):
                    fake_data[field_name] = {f'key{i}': generate_fake_data(field_type.values) for i in range(3)}
                else:
                    fake_data[field_name] = {f'key{i}': generate_fake_data(field_type.values) for i in range(3)}
            elif isinstance(field_type, PrimitiveSchema):
                # Handle primitive fields
                if field_type.type == 'int':
                    fake_data[field_name] = random.randint(0, 100)
                elif field_type.type == 'long':
                    fake_data[field_name] = random.randint(0, 1000000)
                elif field_type.type == 'float':
                    fake_data[field_name] = random.uniform(0, 100)
                elif field_type.type == 'double':
                    fake_data[field_name] = random.uniform(0, 1000000)
                elif field_type.type == 'string':
                    fake_data[field_name] = ''.join(random.choices(['a', 'b', 'c', 'd', 'e', 'f'], k=10))
                elif field_type.type == 'boolean':
                    fake_data[field_name] = True
            elif isinstance(field_type, EnumSchema):
                fake_data[field_name] = random.choice(field_type.symbols)
            elif isinstance(field_type, UnionSchema):
                # Handle nullable union of primitives
                primitive_count = 0
                primitive_list = []
                for n in field_type.schemas:
                    if isinstance(n, PrimitiveSchema):
                        primitive_count +=1
                        if n.type != 'null':
                            primitive_list.append(n.type)
                if primitive_count == len(field_type.schemas):
                    fake_data[field_name] = ''.join(random.choices(['a', 'b', 'c', 'd', 'e', 'f'], k=10))
                    continue
                # Handle union fields
                non_null_types = [t for t in field_type.schemas if t.type != 'null']
                rnd_idx = random.randint(0, len(non_null_types)-1)
                fake_data[field_name] = generate_fake_data(non_null_types[rnd_idx])
            else:
                raise ValueError
    return fake_data


if __name__ == "__main__":
    path = os.path.realpath(os.path.dirname(__file__))
    schema_parse = avro.schema.parse(open(f"{path}/schemas/inputs/schema_complex.json", "rb").read())

    data_payload = generate_fake_data(schema_parse)

    # write to file
    with open(f"{path}/schemas/outputs/schema_complex_payload.json", "w") as f:
        f.write(json.dumps(data_payload, indent=4))
