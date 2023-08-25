import polars as pl

import tiktoken

enc = tiktoken.get_encoding("cl100k_base")

def get_token_length(col):
    return len(enc.encode(col))

new = pl.read_parquet("PMC1800303_PMC1809466.parquet")
old = pl.read_parquet("ref.pq")

print(new == old)
print((old == new).filter(pl.all(pl.col("*"))).height == old.height)
print(new)

new = new.with_columns(num_tokens_abs=pl.col("abstract").apply(get_token_length), num_tokens_main=pl.col("main_text").apply(get_token_length))

print(new)
print(new.select(pl.col("num_tokens_abs").sum(), pl.col("num_tokens_main").sum()))
