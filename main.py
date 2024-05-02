#Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

#Remove extra characters and spaces from column names.
#Replace periods with underscores to avoid issues with column references.
def clean_column_names(df):
    
     # Iterate over each column in the DataFrame
    for old_name in df.columns:
        # Strip leading/trailing spaces, replace backticks, slashes, hyphens, and periods with underscores
        new_name = old_name.strip().replace("`", "").replace("/", "_").replace("-", "_").replace(".", "_")
         # Rename the column in the DataFrame with the new cleaned name
        df = df.withColumnRenamed(old_name, new_name)
    return df     # Return the DataFrame with cleaned column names
#Reducer

def Reducer (df):
    
    # Compute the average stats for each player category using RDDs for custom aggregation.
    # Ensure column names are correct and handle potentially missing columns properly.
    # List of required columns in the DataFrame for processing
    required_columns = [
        'MapsPlayed', #How many maps players have played
        'Kill_Death _Ratio', #The ratio of kills/death
        'DamagePerRound', #Avereage How much damage they did that round out of 100
        'KillsPerRound', #Average kills they get per round
        'SavedTeamatesPerRound',#Average teammates saved per round
        'Impact', #How impactful they were on average
        'Rating_2_0', #A rating system that rates the play on how impactful, usage of utlities, kills, assists, and etc.
        'KAST' #On average how a player get kills, assist, survived, and traded they have gotten
    ]
    
    # Check for missing columns in the DataFrame and raise an error if any are missing
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in the DataFrame: {missing_columns}")
        
 # Convert DataFrame to RDD to use RDD operations for the calculations
    rdd = df.rdd
    
    # Map each row to a tuple consisting of a fixed key and a value tuple with stats
    # Using '1' as a key to aggregate all rows together
    # If an attribute is missing, it defaults to zero
    # Reduce by key (here the key is 1), summing all the individual elements of the tuples
    # This aggregates all the statistics across the entire dataset
    # Calculate the average of each statistic by dividing the total by the count  
    # The results are collected into the driver as a list of tuples and we extract the first result 
    return rdd.map(lambda x: (1, (
        getattr(x, 'MapsPlayed', 0),
        getattr(x, 'Kill_Death _Ratio', 0), 
        getattr(x, 'DamagePerRound', 0),
        getattr(x, 'KillsPerRound', 0),
        getattr(x, 'SavedTeamatesPerRound', 0),
        getattr(x, 'Impact', 0),
        getattr(x, 'Rating_2_0', 0),
        getattr(x, 'KAST', 0),
        1                                      # Add an extra 1 to count the number of rows for the average calculation
    
    ))).reduceByKey(lambda x, y: tuple(x[i] + y[i] for i in range(9)))\
      .map(lambda x: {
        "Avg_MapsPlayed": x[1][0] / x[1][8], 
        "Avg_Kill_Death_Ratio": x[1][1] / x[1][8], 
        "Avg_DamagePerRound": x[1][2] / x[1][8], 
        "Avg_KillsPerRound": x[1][3] / x[1][8], 
        "Avg_SavedTeammatesPerRound": x[1][4] / x[1][8], 
        "Avg_Impact": x[1][5] / x[1][8], 
        "Avg_Rating_2_0": x[1][6] / x[1][8], 
        "Avg_KAST": x[1][7] / x[1][8]
    }).collect()[0]

    # Additionally, compute the same averages using the DataFrame API for comparison
    # Alias each calculation to match the cleaned column names
    stats = df.select(
        avg('MapsPlayed').alias('Avg_MapsPlayed'),
        avg('Kill_Death _Ratio').alias('Avg_Kill_Death_Ratio'),
        avg('DamagePerRound').alias('Avg_DamagePerRound'),
        avg('KillsPerRound').alias('Avg_KillsPerRound'),
        avg('SavedTeamatesPerRound').alias('Avg_SavedTeamatesPerRound'),
        avg('Impact').alias('Avg_Impact'),
        avg('Rating_2_0').alias('Avg_Rating_2_0'),  # Correctly reference the alias to match cleaned column names
        avg('KAST').alias('Avg_KAST')
    ).collect()[0]
    
    # Return a dictionary of column name and average value pairs for both RDD and DataFrame calculations
    return {col: avg_value for col, avg_value in stats.asDict().items()}
    return {col: avg_value for col, avg_value in stats.asDict().items()}
#Mapper

def Mapper(df, avg_stats):
    
    # Define filter conditions for players who meet or exceed the average stats.
    conditions_perfect = (
        (col('MapsPlayed') >= avg_stats['Avg_MapsPlayed']) &
        (col('Kill_Death _Ratio') >= avg_stats['Avg_Kill_Death_Ratio']) &
        (col('DamagePerRound') >= avg_stats['Avg_DamagePerRound']) &
        (col('KillsPerRound') >= avg_stats['Avg_KillsPerRound']) &
        (col('SavedTeamatesPerRound') >= avg_stats['Avg_SavedTeammatesPerRound']) &
        (col('Impact') >= avg_stats['Avg_Impact']) &
        (col('Rating_2_0') >= avg_stats['Avg_Rating_2_0']) &  # Corrected column name
        (col('KAST') >= avg_stats['Avg_KAST']) 
    )

    # Define additional filter conditions based on arbitrary thresholds.
    # These are not necessarily average values but are used to identify another set of 'average' players.
    conditions_average = (
        (col('MapsPlayed') >= 900) &
        (col('Kill_Death _Ratio') >= 1.1) &
        (col('DamagePerRound') >= 80) &
        (col('KillsPerRound') >= 0.77) &
        (col('SavedTeamatesPerRound') >= 0.11) &
        (col('Impact') >= 1.2) &
        (col('Rating_2_0') >= 1.2) &
        (col('KAST') >= 72) 
            )
    # Apply the perfect conditions to filter the DataFrame and create a new DataFrame of qualified players.
    qualified_players_perfect = df.filter(conditions_perfect)
    
    # Apply the average conditions to filter the DataFrame and create another DataFrame of qualified players.
    qualified_players_average = df.filter(conditions_average)
    
    # Return the two DataFrames containing the players who met the perfect and average conditions, respectively.
    return qualified_players_perfect, qualified_players_average
#Driver

def driver():
    
    # Initialize a SparkSession with an application name.
    spark = SparkSession.builder.appName("CSGO Player Analysis").getOrCreate()
    # Define the path to the CSV data in an S3 bucket.
    s3_path = "s3://store-raw-data-dvu/data/csgo_player_stats.csv"
    
    try:
        # Read the CSV file into a DataFrame with headers and inferred schema.
        df = spark.read.csv(s3_path, header=True, inferSchema=True)
        
        # Clean the column names to ensure consistency and ease of access.
        df = clean_column_names(df)
        #df.printSchema()  # Verify the schema after cleaning
        
        # Calculate average statistics using the Reducer function.
        avg_stats = Reducer (df)
        
         # Print out the average statistics for verification.
        print("Average Statistics:")
        for key, value in avg_stats.items():
            print(f"{key}: {value:.2f}")
            
        # Use the Mapper function to filter players based on the computed average statistics.
        qualified_players_perfect, qualified_players_average = Mapper(df, avg_stats)
        
        # Order the average qualified players by descending Rating 2.0 and assign to 'Pros'.
        Pros = qualified_players_average.orderBy(col("` Rating_2.0`").desc())
        
        # Order the perfect qualified players by descending Rating 2.0 and assign to 'Goat'.
        Goat = qualified_players_perfect.orderBy(col("` Rating_2.0`").desc())

        # Specify the S3 buckets where the results will be saved.
        s3_bucket = "s3://transform-data-yml/Result_PotentialGoat.parquet/"
        s3_bucket2 = "s3://transform-data-yml/Result_Goat.parquet/"
        
        # Write the 'Pros' DataFrame to the first S3 bucket in Parquet format, overwriting any existing data.
        Pros.write.mode("overwrite").parquet(s3_bucket)
        # Write the 'Goat' DataFrame to the second S3 bucket in Parquet format, overwriting any existing data.
        Goat.write.mode("overwrite").parquet(s3_bucket2)

        # Read the Parquet files back into DataFrames for verification and display the first 100 rows.
        read = spark.read.parquet(s3_bucket)
        read.show(100)
        read2 = spark.read.parquet(s3_bucket2)
        read2.show(100)
        
    # Catch and print any ValueErrors that occurred during DataFrame operations.
    except ValueError as e:
        print(e)
     # Catch and print any other exceptions that occurred during the execution.
    except Exception as e:
       print("An unexpected error occurred:", e)

driver()

