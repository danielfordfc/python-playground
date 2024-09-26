### Goal

Recreate the glue relationalise function with ruff, uv and polars.
At the same time, I want to pimp my shell.

https://docs.astral.sh/uv/getting-started/features/#the-pip-interface

### What ended up happening

This became pretty tough! So I instead moved to using the relationalise python package - https://github.com/tulip/relationalize

I had some troubles with choice types being inferred throughout the tutorial given (when used on decision_results data)
So I wrote some quick and dirty resolution func to resolve choice types inferred from `.generate_output_columns()`

### Thoughts

- `polars` is incredibly quick and incredibly powerful
- `uv` is very very good as a package manager
- `ruff` is quick and gets a thumbs up from me

The best python appears to be written in rust :)

#### `Relationalize` package

- Incremental by nature
- Works row-by-row which is probably quite slow, but this chunked through a day of decision_results in about 30s, so...
- Being able to search through / fork the code is very useful. Its fairly easy to get your head around.
- Will need to understand how schema evolution will be handled, but having access to the CSVs is very useful!





