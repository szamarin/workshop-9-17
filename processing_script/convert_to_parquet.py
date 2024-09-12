import pandas as pd
import duckdb
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()

    conn = duckdb.connect("temp_data.duckdb")

    # create a table from all the csv files in the input directory
    conn.execute(f"CREATE TABLE temp_table AS SELECT * FROM '{args.input_dir}/*.csv'")

    # convert the temporary table to parquet format
    conn.execute(
        f"""copy (select *, 
    year(TI_LN_DATE_OPEN) as TI_LN_DATE_OPEN_YEAR, 
    month(ti_ln_date_open) as TI_LN_DATE_OPEN_MONTH 
    from temp_table) 
    to '{args.output_dir}' 
    (FORMAT PARQUET, PARTITION_BY (TI_LN_DATE_OPEN_YEAR, TI_LN_DATE_OPEN_MONTH), OVERWRITE_OR_IGNORE true)"""
    )
