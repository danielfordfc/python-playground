import csv
import json
import os
from typing import Dict

from relationalize import Relationalize, Schema
from relationalize.utils import create_local_file

# This example utilizes the local file system as a temporary storage location.

TEMP_OUTPUT_DIR = "output/temp"
FINAL_OUTPUT_DIR = "output/final"
INPUT_DIR = "."

INPUT_FILENAME = "20240824-1.json"
OBJECT_NAME = "decision_results"


def create_iterator(filename):
    with open(filename, "r") as infile:
        for line in infile:
            yield json.loads(line)


def get_objects_from_dir(directory: str):
    for filename in os.listdir(directory):
        yield filename


# 0. Set up file system
os.makedirs(TEMP_OUTPUT_DIR, exist_ok=True)
os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)

# 1. Relationalize raw data
with Relationalize(OBJECT_NAME, create_local_file(output_dir=TEMP_OUTPUT_DIR)) as r:
    r.relationalize(create_iterator(os.path.join(INPUT_DIR, INPUT_FILENAME)))

# Currently having problems resolving choice types

# 2. Generate schemas for each transformed/flattened
schemas: Dict[str, Schema] = {}
for filename in get_objects_from_dir(TEMP_OUTPUT_DIR):
    object_name, _ = os.path.splitext(filename)
    schemas[object_name] = Schema()
    for obj in create_iterator(os.path.join(TEMP_OUTPUT_DIR, filename)):
        schemas[object_name].read_object(obj)
        # https://github.com/tulip/relationalize/blob/main/relationalize/schema.py#L194

# Custom fn 1 to remove choice types.
# def remove_choice_type_from_dict(converted_obj):
#     new_converted_obj = {}
#     field_names = []
#
#     for key, value in converted_obj.items():
#         if key.endswith('_int'):
#             new_key = key[:-4]
#         elif key.endswith('_float'):
#             new_key = key[:-6]
#         else:
#             new_key = key
#
#         new_converted_obj[new_key] = value
#         field_names.append(new_key)
#
#     return new_converted_obj

# Custom fn 2 to remove choice types.
# def process_field_names(field_names):
#     # Use OrderedDict to maintain order and handle duplicates
#     processed = OrderedDict()
#
#     for item in field_names:
#         # Split the item only once from the right
#         parts = item.rsplit('_', 1)
#
#         if len(parts) == 2 and parts[1] in ['int', 'float']:
#             new_item = parts[0]
#         else:
#             new_item = item
#
#         # If the new_item is already in processed, we keep the first occurrence
#         if new_item not in processed:
#             processed[new_item] = None  # We only care about keys, not values

# Convert back to a list
# return list(processed.keys())


def resolve_choice_types(schema_obj):

    """
    Args:
        schema_obj: The Relationalize.Schema object from the source dictionary
    Returns:
        The Same schema object with the Schema.schema object having choices manually resolved.
    """
    for k, v in schema_obj.schema.items():
        # if choice type found of float / int
        if v == "c-float-int":
            schema_obj.schema[k] = "float"
    return schema_obj


# 3. Convert transform/flattened data to prep for database.
#    Generate SQL DDL.
for filename in get_objects_from_dir(TEMP_OUTPUT_DIR):
    object_name, _ = os.path.splitext(filename)

    schema_resolved_choices = resolve_choice_types(schemas[object_name])
    # schemas in the above are of type 'c-float-int', which is a choice...

    field_names = schema_resolved_choices.generate_output_columns()

    # No longer needed as choice types resolved in fn above...
    # processed_field_names = process_field_names(field_names)

    with open(os.path.join(FINAL_OUTPUT_DIR, f"{object_name}.csv"), "w") as out_file:
        writer = csv.DictWriter(out_file, fieldnames=field_names)
        writer.writeheader()
        for obj in create_iterator(os.path.join(TEMP_OUTPUT_DIR, filename)):
            converted_obj = schema_resolved_choices.convert_object(obj)

            # Should now be redundant as the choice types should no longer be there.
            # converted_obj = remove_choice_type_from_dict(converted_obj)

            # converted_obj is what is parsing the types.
            # https://github.com/tulip/relationalize/blob/main/relationalize/schema.py#L53
            writer.writerow(converted_obj)

    with open(
        os.path.join(FINAL_OUTPUT_DIR, f"DDL_{object_name}.sql"), "w"
    ) as ddl_file:
        ddl_file.write(
            schema_resolved_choices.generate_ddl(table=object_name, schema="public")
        )
