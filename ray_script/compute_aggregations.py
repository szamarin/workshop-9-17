import os
import awswrangler as wr
import argparse
import time


if os.environ.get("USE_RAY", "False") == "True":
    from sagemaker_ray_helper import RayHelper
    import ray

    ray_helper = RayHelper()
    ray_helper.start_ray()

    # use modin instead of pandas
    import modin.pandas as pd

    # configure wrangler to use ray and modin
    wr.engine.set("ray")
    wr.memory_format.set("modin")

else:

    import pandas as pd

    wr.engine.set("python")
    wr.memory_format.set("pandas")


if __name__ == "__main__":

    # parse the input arguments
    args = argparse.ArgumentParser()
    args.add_argument("--input_data_location", type=str)
    args.add_argument("--output_data_location", type=str)

    parsed_args = args.parse_args()

    input_data_location = parsed_args.input_data_location
    output_data_location = parsed_args.output_data_location

    # read data
    t0 = time.perf_counter()
    df = wr.s3.read_csv(input_data_location)
    read_time = time.perf_counter() - t0
    print(
        f"########################################### File read from S3 in: {read_time}s ###########################################"
    )

    # SIMPLE AGGREGATION
    # compute average account balance, average minimum payment, and arrears for each customer's account
    t0 = time.perf_counter()
    account_aggregation = (
        df.groupby(["TI_CU_CUSTOMER_ID", "TI_LN_ACCOUNT_ID"])
        .agg(
            AVERAGE_ACCOUNT_BALANCE=("TI_LN_BALANCE", "mean"),
            AVERAGE_MIN_PAYMENT=("TI_LN_VAL_PAYMENTS", "mean"),
            LATE_PAYMENTS=("TI_LN_NUM_MTHS_IN_ARREARS", "sum"),
        )
        .reset_index()
    )
    account_aggregation_time = time.perf_counter() - t0
    print(
        f"########################################### Account aggregation completed in: {account_aggregation_time}s ###########################################"
    )

    # MORE COMPLEX AGGREGATION
    # compute monthly balances, active accounts, and arrears for each customer
    t0 = time.perf_counter()
    df["TI_LN_DATE_OPEN"] = pd.to_datetime(df["TI_LN_DATE_OPEN"])
    df["months_elapsed"] = df["TI_LN_ORIGINAL_TERM"] - df["TI_LN_REMAINING_TERM"]
    df["payment_date"] = df.apply(
        lambda row: row["TI_LN_DATE_OPEN"]
        + pd.DateOffset(months=row["months_elapsed"]),
        axis=1,
    )
    monthly_balances = (
        df.groupby(["TI_CU_CUSTOMER_ID", df["payment_date"].dt.to_period("M")])
        .agg(
            TOTAL_MOPNTHLY_BALANCE=("TI_LN_BALANCE", "sum"),
            TOTAL_ARREARS=("TI_LN_NUM_MTHS_IN_ARREARS", "sum"),
            NUM_ACCOUNTS=("TI_LN_ACCOUNT_ID", "count"),
        )
        .reset_index()
    )
    monthly_balance_time = time.perf_counter() - t0
    print(
        f"########################################### Monthly balance aggregation completed in: {monthly_balance_time}s ###########################################"
    )

    monthly_balances["payment_date"] = monthly_balances[
        "payment_date"
    ].dt.to_timestamp()

    wr.s3.to_csv(account_aggregation, output_data_location + "/account_aggregation.csv")
    wr.s3.to_csv(monthly_balances, output_data_location + "/monthly_balances.csv")

    # gracefully shutdown cluster
    if os.environ.get("USE_RAY", "False") == "True":
        ray.shutdown()
