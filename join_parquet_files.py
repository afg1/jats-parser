import polars as pl
from pathlib import Path

import click



@click.command()
@click.argument("input_path", type=click.Path(exists=True))
@click.argument("output", type=click.Path())
def main(input_path, output):
    all_pq = sorted(Path(input_path).glob("*.parquet"))

    complete_pq = pl.comcat([pl.scan_parquet(pq) for pq in all_pq])

    complete_pq.sink_parquet(output)    


