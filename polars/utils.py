import polars as pl


def explode_s3_reference_struct(df: pl.DataFrame) -> pl.DataFrame:
    # Explode the s3_reference struct column
    exploded_df = df.with_columns(
        [
            pl.col("s3_reference")
            .struct.field("key")
            .str.split("/")
            .list.get(3)
            .alias("decision_timestamp"),
            pl.col("s3_reference")
            .struct.field("key")
            .str.split("/")
            .list.get(1)
            .alias("loan_application_id"),
        ]
    )

    # Drop the original s3_reference column
    exploded_df = exploded_df.drop("s3_reference")

    return exploded_df
