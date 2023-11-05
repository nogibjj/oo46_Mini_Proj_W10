import mylib.lib as lib
import matplotlib.pyplot as plt


def main():
    # Create Spark session
    spark = lib.create_spark_session("DataProcessingApp")

    # Columns to select
    columns_to_select = [
        "country",
        "country_long",
        "name",
        "capacity_mw",
        "primary_fuel",
    ]

    # Load data and select only the specified columns
    file_path = "dataset/global_power_plant_database.csv"
    df = lib.load_data(spark, file_path, columns=columns_to_select)

    # Describe data
    df_clean = df.na.drop()

    # Select only the capacity_mw column
    df_clean = df_clean.select("capacity_mw")
    description = lib.describe_data(df_clean)
    print("Description of capacity_mw column:")
    print(description, "\n")

    # Perform query
    # unique_values = lib.simple_query(df, "column_of_interest")
    # unique_values.show()
    top_countries_df, bottom_countries_df = lib.top_countries_by_capacity(df)

    print("Top 10 countries by capacity:")
    top_countries_df.show()

    print("Bottom 10 countries by capacity:")
    bottom_countries_df.show()

    # Convert Spark DataFrame to Pandas DataFrame
    top_countries_pd = top_countries_df.toPandas()

    # Plotting
    plt.figure(figsize=(10, 8))
    plt.barh(top_countries_pd["COUNTRY"], top_countries_pd["TOTAL CAPACITY"])
    plt.xlabel("Total Capacity (MW)")
    plt.ylabel("Country")
    plt.title("Top 10 Countries by Electricity Generation Capacity")
    plt.gca().invert_yaxis()  # To display the highest value at the top
    plt.show()

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
