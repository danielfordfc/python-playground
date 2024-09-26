from typing import List, Union, Tuple
from pprint import pprint
from utils import explode_s3_reference_struct
import polars as pl


def relationalize(
    df: Union[pl.DataFrame, pl.LazyFrame],
    parent_key: str = "",
    parent_id: str = "id",
    depth: int = 0,
) -> List[pl.DataFrame]:
    print(f"{'  ' * depth}Entering relationalize with parent_key: {parent_key}")

    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    frames = []
    columns_to_drop = []

    def process_struct_column(df: pl.DataFrame, column: str) -> pl.DataFrame:
        print(f"{'  ' * (depth + 1)}Processing struct column: {column}")
        # Need to actually decide what I want to do here with struct columns....

        struct_fields = df.select(pl.col(column)).to_series().drop_nulls()[0].keys()
        new_columns = [
            pl.col(column).struct.field(field).alias(f"{column}_{field}")
            for field in struct_fields
        ]
        df = df.with_columns(new_columns)
        columns_to_drop.append(column)
        return df

    def process_list_column(
        df: pl.DataFrame, column: str
    ) -> Tuple[pl.DataFrame, List[pl.DataFrame]]:
        print(f"{'  ' * (depth + 1)}Processing list column: {column}")
        new_frames = []
        sample = df.select(pl.col(column)).to_series().drop_nulls()

        if len(sample) > 0:
            list_df = df.select(
                pl.col(parent_id), pl.col(column).alias("temp")
            ).explode("temp")

            if (
                isinstance(sample[0], list)
                and len(sample[0]) > 0
                and isinstance(sample[0][0], dict)
            ):
                print(
                    f"{'  ' * (depth + 2)}List column {column} contains dicts, recursing..."
                )
                list_df = list_df.with_columns(pl.col("temp").cast(pl.Struct))
                new_frames = relationalize(
                    list_df, parent_key=column, parent_id=parent_id, depth=depth + 1
                )
            else:
                print(
                    f"{'  ' * (depth + 2)}List column {column} contains non-dict values or empty lists"
                )
                list_df = list_df.rename({"temp": column})
                new_frames = [list_df]

        print(f"{'  ' * (depth + 2)}New frames created for {column}: {len(new_frames)}")
        columns_to_drop.append(column)
        return df, new_frames

    print(f"{'  ' * depth}DataFrame columns: {df.columns}")

    for column in df.columns:
        if df[column].dtype == pl.Struct:
            df = process_struct_column(df, column)
        elif df[column].dtype == pl.List:
            df, new_frames = process_list_column(df, column)
            print(
                f"{'  ' * (depth + 1)}Extending frames with {len(new_frames)} new frames from {column}"
            )
            frames.extend(new_frames)

    df = df.drop(columns_to_drop)
    print(f"{'  ' * depth}Columns dropped: {columns_to_drop}")

    frames.insert(0, df)
    print(f"{'  ' * depth}Total frames after processing: {len(frames)}")

    return frames


# Create a Polars DataFrame from the example data
# parse the test.json file into a polars DataFrame
input_df = pl.read_json("test.json")

# Explode the s3_reference struct column
input_df = explode_s3_reference_struct(input_df)

# Call the relationalize function
result_frames = relationalize(input_df)

# Print the resulting DataFrames
for i, df in enumerate(result_frames):
    print(f"DataFrame {i}:")
    print(pprint(df.collect_schema()))
    print(df)
    print("\n")
