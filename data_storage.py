"""
Data Storage Integration Example for FinanceLake

This module demonstrates how to integrate with the Delta Lake storage layer
using Apache Spark for reading, writing, and querying financial data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, lit, current_date
from delta import DeltaTable
from typing import Optional, Dict, List
import os

class FinanceLakeStorage:
    """
    FinanceLake storage layer integration class.

    Provides methods for reading from and writing to Delta Lake tables
    with optimized partitioning and performance features.
    """

    def __init__(self, spark: SparkSession, base_path: str = "./data/financelake"):
        """
        Initialize the storage layer.

        Args:
            spark: SparkSession instance
            base_path: Base path for the data lake (local path for testing, S3 for production)
        """
        self.spark = spark
        self.base_path = base_path

        # Create base directory if it doesn't exist
        os.makedirs(base_path, exist_ok=True)

        # Configure Delta Lake settings
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        self.spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

    def write_bronze_layer(self, df, table_name: str, partition_cols: Optional[List[str]] = None):
        """
        Write data to the bronze layer.

        Args:
            df: Spark DataFrame to write
            table_name: Name of the table
            partition_cols: Columns to partition by
        """
        path = f"{self.base_path}/bronze/{table_name}"

        if partition_cols is None:
            partition_cols = ["date", "region"]

        # Ensure partitioning columns exist
        df_transformed = df

        # Add date column if it doesn't exist
        if "date" not in df.columns:
            if "transaction_date" in df.columns:
                df_transformed = df_transformed.withColumn("date", col("transaction_date").cast("date"))
            else:
                # Create a default date if no date column exists
                from pyspark.sql.functions import current_date
                df_transformed = df_transformed.withColumn("date", current_date())

        # Add region column if it doesn't exist
        if "region" not in df.columns:
            if "country" in df.columns:
                df_transformed = df_transformed.withColumn("region", col("country"))
            else:
                # Default region if no region or country column
                df_transformed = df_transformed.withColumn("region", lit("UNKNOWN"))

        # Filter out only existing partition columns
        existing_partition_cols = [col for col in partition_cols if col in df_transformed.columns]

        # Write with Delta format and partitioning
        writer = df_transformed.write.format("delta").mode("overwrite")

        if existing_partition_cols:
            writer = writer.partitionBy(*existing_partition_cols)

        writer.save(path)

        print(f"Data written to bronze layer: {path}")

    def read_bronze_layer(self, table_name: str, filters: Optional[Dict[str, str]] = None):
        """
        Read data from the bronze layer.

        Args:
            table_name: Name of the table to read
            filters: Optional filters to apply

        Returns:
            Spark DataFrame
        """
        path = f"{self.base_path}/bronze/{table_name}"

        try:
            df = self.spark.read.format("delta").load(path)
        except Exception as e:
            print(f"Warning: Could not read table {table_name}: {e}")
            # Return empty DataFrame with basic schema
            return self.spark.createDataFrame([], "id: string")

        if filters:
            for col_name, value in filters.items():
                if col_name in df.columns:
                    df = df.filter(col(col_name) == value)

        return df

    def transform_to_silver(self, bronze_table: str, silver_table: str):
        """
        Transform data from bronze to silver layer with business logic.

        Args:
            bronze_table: Source bronze table name
            silver_table: Target silver table name
        """
        # Read bronze data
        bronze_df = self.read_bronze_layer(bronze_table)

        # Apply business transformations
        silver_df = bronze_df

        # Add date-related columns if date column exists
        if "date" in bronze_df.columns:
            silver_df = silver_df \
                .withColumn("year", year(col("date"))) \
                .withColumn("month", month(col("date"))) \
                .withColumn("day", dayofmonth(col("date")))

        # Filter valid amounts if amount column exists
        if "amount" in bronze_df.columns:
            silver_df = silver_df \
                .filter(col("amount").isNotNull()) \
                .filter(col("amount") > 0)

        # Write to silver layer with Z-ordering for performance
        path = f"{self.base_path}/silver/{silver_table}"

        # Determine partition columns that exist
        partition_cols = []
        if "year" in silver_df.columns:
            partition_cols.extend(["year", "month"])
        if "region" in silver_df.columns:
            partition_cols.append("region")

        writer = silver_df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(path)

        # Optimize with Z-ordering if customer_id and transaction_type exist
        try:
            delta_table = DeltaTable.forPath(self.spark, path)
            zorder_cols = []
            if "customer_id" in silver_df.columns:
                zorder_cols.append("customer_id")
            if "transaction_type" in silver_df.columns:
                zorder_cols.append("transaction_type")

            if zorder_cols:
                delta_table.optimize().executeZOrderBy(*zorder_cols)
        except Exception as e:
            print(f"Warning: Z-ordering optimization failed: {e}")

        print(f"Data transformed to silver layer: {path}")

    def create_gold_aggregations(self, silver_table: str, gold_table: str):
        """
        Create aggregated data for the gold layer.

        Args:
            silver_table: Source silver table name
            gold_table: Target gold table name
        """
        path = f"{self.base_path}/silver/{silver_table}"
        silver_df = self.spark.read.format("delta").load(path)

        # Create daily summaries - check which columns exist
        group_cols = []
        if "date" in silver_df.columns:
            group_cols.append("date")
        if "region" in silver_df.columns:
            group_cols.append("region")
        if "transaction_type" in silver_df.columns:
            group_cols.append("transaction_type")

        if not group_cols:
            group_cols = ["date"]  # fallback

        # Build aggregation expressions based on available columns
        agg_expr = {}
        if "amount" in silver_df.columns:
            agg_expr["amount"] = "sum"
        if "customer_id" in silver_df.columns:
            agg_expr["customer_id"] = "count"

        if not agg_expr:
            # Fallback aggregation if no suitable columns
            gold_df = silver_df.groupBy(*group_cols).count()
        else:
            gold_df = silver_df.groupBy(*group_cols).agg(agg_expr)

            # Rename columns for clarity
            if "sum(amount)" in gold_df.columns:
                gold_df = gold_df.withColumnRenamed("sum(amount)", "total_amount")
            if "count(customer_id)" in gold_df.columns:
                gold_df = gold_df.withColumnRenamed("count(customer_id)", "transaction_count")

        # Write to gold layer
        gold_path = f"{self.base_path}/gold/{gold_table}"

        # Determine partition columns
        partition_cols = []
        if "date" in gold_df.columns:
            partition_cols.append("date")
        if "region" in gold_df.columns:
            partition_cols.append("region")

        writer = gold_df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(gold_path)

        print(f"Gold layer aggregations created: {gold_path}")

    def query_with_optimization(self, table_path: str, query_filters: Dict[str, str]):
        """
        Perform optimized queries with partition pruning and caching.

        Args:
            table_path: Path to the Delta table
            query_filters: Dictionary of filters to apply

        Returns:
            Spark DataFrame with query results
        """
        df = self.spark.read.format("delta").load(table_path)

        # Apply filters for partition pruning
        for col_name, value in query_filters.items():
            df = df.filter(col(col_name) == value)

        # Cache frequently accessed data
        df.cache()

        return df

    def perform_maintenance(self, table_path: str):
        """
        Perform maintenance operations on Delta tables.

        Args:
            table_path: Path to the Delta table
        """
        delta_table = DeltaTable.forPath(self.spark, table_path)

        # Optimize file sizes
        delta_table.optimize().executeCompaction()

        # Remove old versions (keep last 7 days)
        delta_table.vacuum(7)

        print(f"Maintenance completed for: {table_path}")

    def time_travel_query(self, table_path: str, version: Optional[int] = None, timestamp: Optional[str] = None):
        """
        Query historical data using Delta Lake time travel.

        Args:
            table_path: Path to the Delta table
            version: Version number to query
            timestamp: Timestamp to query (format: "2024-01-01 00:00:00")

        Returns:
            Spark DataFrame with historical data
        """
        options = {}
        if version is not None:
            options["versionAsOf"] = str(version)
        elif timestamp is not None:
            options["timestampAsOf"] = timestamp

        df = self.spark.read.format("delta").options(**options).load(table_path)
        return df

    def backup_table(self, table_path: str, backup_path: str):
        """
        Create a backup of a Delta table.

        Args:
            table_path: Source table path
            backup_path: Backup destination path
        """
        # Copy the entire table directory
        self.spark.read.format("delta").load(table_path) \
            .write.format("delta").mode("overwrite").save(backup_path)

        print(f"Backup created: {backup_path}")


def main():
    """
    Example usage of the FinanceLake storage layer.
    """
    # Initialize Spark session with Delta Lake support
    spark = SparkSession.builder \
        .appName("FinanceLake Storage Example") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()

    # Initialize storage layer with local path for testing
    storage = FinanceLakeStorage(spark, "./data/financelake")

    try:
        # Example: Write sample data to bronze layer
        sample_data = [
            {"transaction_id": "TXN001", "customer_id": "CUST001", "amount": 1000.0,
             "transaction_date": "2024-01-01", "country": "US", "transaction_type": "DEPOSIT"},
            {"transaction_id": "TXN002", "customer_id": "CUST002", "amount": 500.0,
             "transaction_date": "2024-01-01", "country": "EU", "transaction_type": "WITHDRAWAL"},
            {"transaction_id": "TXN003", "customer_id": "CUST001", "amount": 200.0,
             "transaction_date": "2024-01-02", "country": "US", "transaction_type": "DEPOSIT"},
        ]

        df = spark.createDataFrame(sample_data)
        storage.write_bronze_layer(df, "transactions")

        # Example: Transform to silver layer
        storage.transform_to_silver("transactions", "enriched_transactions")

        # Example: Create gold layer aggregations
        storage.create_gold_aggregations("enriched_transactions", "daily_summaries")

        # Example: Optimized query
        results = storage.query_with_optimization(
            f"{storage.base_path}/gold/daily_summaries",
            {"region": "US"}
        )
        print("Query results:")
        results.show()

        # Example: Time travel query (if table exists)
        try:
            historical_data = storage.time_travel_query(
                f"{storage.base_path}/silver/enriched_transactions",
                version=0
            )
            print("Historical data:")
            historical_data.show()
        except Exception as e:
            print(f"Time travel query failed: {e}")

        # Example: Maintenance
        try:
            storage.perform_maintenance(f"{storage.base_path}/silver/enriched_transactions")
        except Exception as e:
            print(f"Maintenance failed: {e}")

        print("FinanceLake storage example completed successfully!")

    except Exception as e:
        print(f"Error during execution: {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()