library(arrow)
library(dplyr)


seattle_csv <- open_dataset(
  sources = "data/seattle-library-checkouts-tiny.csv", 
  col_types = schema(ISBN = string()),
  format = "csv"
)


nrow(seattle_csv)


seattle_csv |>
    head(20) |>
    mutate(IsBook = endsWith(MaterialType, "BOOK")) |>
    select(MaterialType, IsBook) |>
    collect()

seattle_csv |>
    filter(endsWith(MaterialType, "BOOK")) |>
    group_by(CheckoutYear) |>
    summarize(Checkouts = sum(Checkouts)) |>
    arrange(CheckoutYear) |>
    collect() |>
    system.time()

seattle_parquet_dir <- "./data/seattle-library-checkout-parquet"

seattle_csv |> 
    write_dataset(path = seattle_parquet_dir, format = "parquet")

open_dataset(sources = seattle_parquet_dir, 
    format = "parquet") |>
    filter(endsWith(MaterialType, "BOOK")) |>
    group_by(CheckoutYear) |>
    summarize(Checkouts = sum(Checkouts)) |>
    arrange(CheckoutYear) |>
    collect() |>
    system.time()


seattle_parquet_part <- "./data/seattle-library-checkouts"

seattle_csv |>
    group_by(CheckoutYear) |>
    write_dataset(path = seattle_parquet_part, format = "parquet")

open_dataset(sources = seattle_parquet_part,
format = "parquet") |>
filter(endsWith(MaterialType, "BOOK")) |>
    group_by(CheckoutYear) |>
    summarize(Checkouts = sum(Checkouts)) |>
    arrange(CheckoutYear) |>
    collect() |>
    system.time()

open_dataset(sources = seattle_parquet_part,
format = "parquet") |>
filter(CheckoutYear == 2020,
    endsWith(MaterialType, "BOOK")) |>
    group_by(CheckoutMonth) |>
    summarize(Checkouts = sum(Checkouts)) |>
    arrange(CheckoutMonth) |>
    collect() |>
    system.time()

