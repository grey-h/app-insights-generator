import argparse

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

import itertools
import time
import traceback
from functools import reduce

"""
dataframe contains below columns:  ['_c0',
                                     'appId',
                                     'developer',
                                     'developerId',
                                     'developerWebsite',
                                     'free',
                                     'genre',
                                     'genreId',
                                     'inAppProductPrice',
                                     'minInstalls',
                                     'offersIAP',
                                     'originalPrice',
                                     'price',
                                     'ratings',
                                     'len screenshots',
                                     'adSupported',
                                     'containsAds',
                                     'reviews',
                                     'releasedDayYear',
                                     'sale',
                                     'score',
                                     'summary',
                                     'title',
                                     'updated',
                                     'histogram1',
                                     'histogram2',
                                     'histogram3',
                                     'histogram4',
                                     'histogram5',
                                     'releasedDay',
                                     'releasedYear',
                                     'releasedMonth',
                                     'dateUpdated',
                                     'minprice',
                                     'maxprice',
                                     'ParseReleasedDayYear'
                                    ]

will use only a few to get the insights out of the dataframe.
"""

# After some consideration like either column not having enough values or all of the values were null , decided to
# provide insights on below columns.
columns_to_provide_insights = [
    "developer",
    "developerWebsite",
    "free",
    "genre",
    "minInstalls",
    "offersIAP",
    "originalPrice",
    "price",
    "ratings",
    "len screenshots",
    "adSupported",
    "containsAds",
    "reviews",
    "score",
    "releasedYear",
]


# will use those Id columns for aggregation, and value columns to show into actual CSV to the user
id_to_value_mapping = {
    # Assumption that developerId is key, it will provide info about developer and developerWebsite
    # Assuming because there can be developer with same name and different developerId, similarly for developerWebsite
    "developerId": ["developer", "developerWebsite"],
    # Assumption that genreId is key, it will provide info about developer
    "genreId": ["genre"]
    # TODO: if two genre having same name and different genreId - this will lead to confusion
}
id_columns = list(id_to_value_mapping.keys())

# extracted this from columns_to_provide_insights list
cols_for_aggregations = [
    "developerId",
    "free",
    "genreId",
    "minInstalls",
    "offersIAP",
    "originalPrice",
    "price",
    "ratings",
    "len screenshots",
    "adSupported",
    "containsAds",
    "reviews",
    "score",
    "releasedYear",
]

# decided this bucket size after taking a look at the given data.
numeric_cols_to_bucket_size_mapping = {
    "minInstalls": 1000000,
    "originalPrice": 100,
    "price": 100,
    "ratings": 1000000,
    "len screenshots": 5,
    "reviews": 1000000,
    "score": 1,
    "releasedYear": 5,
}
numeric_cols = list(numeric_cols_to_bucket_size_mapping.keys())

threshold_percent = 2
default_parallelism = 128  # Limit for parallel union operation
num_partitions = 2

logging_tag = "[++PlayStoreDataExtractor++]"


def load_spark():
    """
    Initializes and returns a PySpark SparkSession with specified configurations.
    :return: pyspark.sql.session.SparkSession: The Spark session.
    """
    conf = SparkConf()
    spark_options = dict()
    spark_options["fs.s3a.bucket.qstar.endpoint.region"] = "ap-south-1"
    spark_options["spark.serializer"] = "org.apache.spark.serializer.KryoSerializer"
    spark_options["spark.dynamicAllocation.enabled"] = "true"

    # Set Spark configurations based on provided options
    for k, v in spark_options.items():
        conf.set(k, v)

    spark = (SparkSession
             .builder
             .appName("PlayStore Data Extractor")
             .config(conf=conf)
             .getOrCreate()
             )
    spark.sparkContext.setLogLevel("WARN")

    return spark


def do_column_type_casting(df, col_list, ):
    """
    Convert string type columns to Integer types.
    :param df: pyspark dataframe
    :param col_list: list of column names
    :return: pyspark dataframe
    """
    for col in col_list:
        df = df.withColumn(col, F.col(col).cast(IntegerType()))
    return df


def do_bucketing(df, cols_to_bucket, col_to_bucket_size_mapping):
    """
    Apply bucketing to specified columns in a pyspark dataframe.
    :param df: pyspark dataframe
        ex: +----+----+
            |col1|col2|
            +----+----+
            |   1|   1|
            |   2|   2|
            |   3|   3|
            |   4|   4|
            |   5|   5|
            |   6|   6|
            |   7|   7|
            |   8|   8|
            |   9|   9|
            +----+----+
    :param cols_to_bucket: list of column names to buckets, ex: ["col1", "col2"]
    :param col_to_bucket_size_mapping: dictionary mapping column names, ex: {"col1": 2, "col2": 5}
    :return: pyspark dataframe
        ex: +----+----+
            |col1|col2|
            +----+----+
            |   0|   1|
            |   2|   2|
            |   2|   3|
            |   4|   4|
            |   4|   5|
            |   6|   6|
            |   6|   7|
            |   8|   8|
            |   8|   9|
            +----+----+
    """
    for each_col in cols_to_bucket:
        # not handling case when bucket_size of col is not present, not providing any default value
        # Ensure bucket size is defined for the column
        bucket_size = col_to_bucket_size_mapping[each_col]
        bucket_expr = F.expr(f"floor(`{each_col}` / {bucket_size}) * {bucket_size}")
        df = df.withColumn(each_col, bucket_expr)
    return df


def filter_playstore_df(df):
    """
    Filters a PySpark DataFrame containing playstore data based on predefined criteria.
    considering rating to be on the scale of 0-100, found some values like 166417449 in the given data set
    considering release year to be from 1971 to current year
    :param df: pyspark.dataframe
    :return: pyspark.dataframe
    """
    df = (df
          .filter("ratings>=0 and ratings<=100")
          .filter("releasedYear>=1971 and releasedYear<=2023")
          )
    return df


def read_csv_file(spark, path, header=False, inferSchema=False, num_partitions=num_partitions):
    """
    Reads a CSV file into a PySpark DataFrame.
    :param spark: pyspark.sql.session.SparkSession, The Spark session
    :param path: str,The path to the CSV file.
    :param header: boolean
    :param inferSchema: boolean
    :param num_partitions: int, The number of partitions
    :return: pyspark.dataframe
    """
    raw_df = (spark
              .read
              .option("header", header)
              .option("inferSchema", inferSchema)
              .option("numPartitions", num_partitions)
              .csv(path)
              )
    return raw_df


def write_to_csv(df, path, header=False, mode="append"):
    """
    Write a PySpark DataFrame to a CSV file.
    :param df: pyspark.dataframe
    :param path: str, The path to the output CSV file
    :param header: boolean, Whether to include a header in the CSV file. Default is False.
    :param mode: str, The behavior when writing to the CSV file. Options are "append", "overwrite". Default is "append".
    """
    print(f"{logging_tag} Writing to csv, path: {path}")
    (df
     .write
     .option("header", header)
     .mode(mode)
     .csv(path)
     )
    print(f"{logging_tag} Done writing to csv, path: {path}")


def get_insights_from_df(df, insights_parameters, df_count=None):
    """
    Generate insights from a PySpark DataFrame based on specified insights_parameters.
    :param df: pyspark.dataframe
    :param insights_parameters: List of columns to consider for generating insights.
    :param df_count: Count of rows in the DataFrame. If not provided, it will be calculated.
    :return: pyspark.dataframe

    The function performs the following steps:
    1. Filters the DataFrame based on non-null values in specified insights_parameters.
    2. Groups the DataFrame by insights_parameters and aggregates values.
    3. Applies a threshold filter to retain only groups with counts greater than or equal to a specified percentage.
    4. Drops unnecessary ID columns from the result DataFrame.
    5. Converts numeric columns to bucket format and other columns to "column=<column_value>" format.
    6. Concatenates the columns into a single "Insights" column separated by a specified delimiter.

    """
    if df_count is None:
        df_count = df.count()

    # Calculate the threshold count based on a percentage of total rows
    threshold_count = int((threshold_percent / 100) * df_count)

    pick_id_to_val_columns = []
    id_columns_in_insights_parameters = [x for x in insights_parameters if x in id_columns]
    for id_col in id_columns_in_insights_parameters:
        pick_id_to_val_columns.extend([F.first(val_col).alias(val_col) for val_col in id_to_value_mapping.get(id_col, [])])

    # Perform grouping and aggregation on specified columns
    df = (df.filter(*[reduce(lambda x, y: x & y, (F.col(agg_col).isNotNull() for agg_col in insights_parameters))])
          .groupBy(*insights_parameters)
          .agg(*pick_id_to_val_columns, F.count('*').alias("count"))
          .filter(f'count>={threshold_count}')
          .drop(*id_columns_in_insights_parameters)
          )

    for current_col in df.columns:
        if current_col == "count":
            continue
        # Convert numeric columns to the bucket format, e.g., convert 100 to [100-200]
        if current_col in numeric_cols:
            bucket_size = numeric_cols_to_bucket_size_mapping[current_col]
            df = df.withColumn(current_col, F.concat(F.lit("["),
                                                     F.col(current_col), F.lit("-"), F.col(current_col) + bucket_size,
                                                     F.lit("]")
                                                     )
                               )
        # converting columns to column=<column_value> format
        df = df.withColumn(current_col, F.concat(F.lit(current_col), F.lit('='), F.col(current_col)))

    parameter_delimiter_in_output = ";"
    # Concatenate all columns into a single "Insights" column
    insights_df = (df
                   .withColumn("Insights", F.concat_ws(parameter_delimiter_in_output, *df.columns))
                   .select("Insights")
                   )
    return insights_df


def union_all_dfs(dfs):
    """
    Combine multiple PySpark DataFrames into a single DataFrame using union operation.
    :param dfs: list of DataFrames (pyspark.dataframe)
    :return: A single PySpark DataFrame resulting from the union of input DataFrames
    """
    union_df = dfs[0] if len(dfs) == 1 else reduce(lambda df1, df2: df1.unionAll(df2), dfs)
    return union_df


def get_insights_from_df_with_possible_combinations_parallely(df, insights_parameters_list, output_path):
    """
    Extracts insights from a PySpark DataFrame by computing aggregations for all possible combinations
    of specified insights parameters, utilizing parallel processing.
    :param df: pyspark.dataframe, Input PySpark DataFrame.
    :param insights_parameters_list: List of insights parameters (column names) for aggregations.
    :param output_path: Path to the output CSV file for storing insights.

    The function performs the following steps:
    1. Counts the number of rows in the input DataFrame.
    2. Divides the computation into combinations of insights parameters with varying sizes.
    3. Utilizes parallel processing to compute insights for each combination.
    4. Combines the individual DataFrames into a single result DataFrame using union.
    5. Writes the result DataFrame to a CSV file.

    """
    df_count = df.count()
    for size in range(1, len(insights_parameters_list)+1):
        start_time = time.time()
        print(f"{logging_tag} Starting to get insights from insight_size: {size}, out of {len(insights_parameters_list)}")
        insights_parameters_combinations = list(itertools.combinations(insights_parameters_list, size))
        combinations_count = len(insights_parameters_combinations)
        for i in range(0, combinations_count, default_parallelism):
            print(f"{logging_tag} Starting to get insights from insight_size: {size}, out of {len(insights_parameters_list)}, i: {i} out of {combinations_count}")
            # Generate insights for each combination in parallel
            insights_dfs = [get_insights_from_df(df, each_combination, df_count) for each_combination in
                            insights_parameters_combinations[i:min(i + default_parallelism, combinations_count)]]
            if insights_dfs:
                result_df = union_all_dfs(insights_dfs)
                print(f"{logging_tag} done reducing, writing into CSV")
                write_to_csv(result_df.coalesce(1), output_path, True)
        end_time = time.time()
        print(f"{logging_tag} Done for insight_size: {size}, time_taken: {end_time-start_time}")


def extract_data(csv_file_path, output_file_path):
    """
    Extracts, transforms, and analyzes data from a CSV file using PySpark.
    :param csv_file_path: string path to the CSV file
    :param output_file_path: string path to the output CSV file
    :return: pyspark.dataframe
    The function performs the following steps:
    1. Loads a PySpark DataFrame from the specified CSV file.
    2. Converts specified numeric columns to the Integer type.
    3. Filters the DataFrame based on specific conditions.
    4. Applies bucketing to numeric columns with specified bucket sizes.
    5. Caches the bucketed DataFrame for optimization.
    6. Computes insights from the DataFrame with possible combinations of columns using parallel processing.
    7. Prints insights or writes them to an output CSV file if provided.
    8. Finally, unpersists the cached DataFrame.

    """
    start_time = time.time()
    print(f"{logging_tag} Starting to generate insights, start_time={start_time}")
    spark = load_spark()
    raw_df = read_csv_file(spark, csv_file_path, True, True)

    numeric_df = do_column_type_casting(raw_df, numeric_cols)
    numeric_df = filter_playstore_df(numeric_df)

    bucketed_df = do_bucketing(numeric_df, numeric_cols, numeric_cols_to_bucket_size_mapping).repartition(num_partitions)
    try:
        bucketed_df = bucketed_df.cache()
        get_insights_from_df_with_possible_combinations_parallely(bucketed_df, cols_for_aggregations, output_file_path+"_partitioned")
        insight_df = read_csv_file(spark, output_file_path+"_partitioned", True, True)
        write_to_csv(insight_df.coalesce(1), output_file_path, True)
        print(f"{logging_tag} Insights have been written to {output_file_path}")
    except Exception as e:
        print(f"{logging_tag} Exception Occurred, Exception: {e}")
        print(traceback.format_exc())
    finally:
        bucketed_df.unpersist()

    end_time = time.time()
    print(f"{logging_tag} Done generating insights, time taken: {end_time-start_time}")

    return raw_df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process CSV file and generate insights.")

    parser.add_argument('--csv_file_path', type=str, default="s3a://qstar/Assessments/segwise/playstore.csv",
                        help="Path to the input CSV file.")
    parser.add_argument('--output_path', type=str, default="s3a://qstar/Assessments/segwise/insights",
                        help="Path to the output directory for insights.")
    args = parser.parse_args()
    csv_file_path = args.csv_file_path
    output_path = args.output_path

    print(f"{logging_tag} csv_file_path: {csv_file_path}")
    print(f"{logging_tag} output_path: {output_path}")

    extract_data(csv_file_path, output_path)
