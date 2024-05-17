df = (df.withColumn("value",
        from_json(decode("value",charset="UTF-8"),schema=schema)
        .alias("value"))
        .select("value.*")
    )
# Create an empty DataFrame with the same schema as the original DataFrame
empty_df = spark.createDataFrame([], schema=schema)
df = empty_df