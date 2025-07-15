from pyspark.sql import SparkSession
from datetime import datetime
import os
from pyspark.sql.functions import to_timestamp, col, row_number, regexp_replace, trim, instr, lower,when, current_timestamp, datediff
from pyspark.sql.window import Window
import re
from pyspark.sql.functions import udf, length, to_date
from pyspark.sql.types import StringType


# Define S3 paths first
S3_BUCKET = 's3a://raw-youtube-data-9'
RAW_PREFIX = 'raw_data/'
PROCESSED_PREFIX = 'youtube_processed/'

# Generate today's filename
today = datetime.utcnow().strftime('%Y-%m-%d')
raw_key = f"{RAW_PREFIX}*.json"

# Optional: dynamically resolve path
jar_dir = os.path.join(os.path.dirname(__file__), "jars")
jars = ",".join([
    os.path.join(jar_dir, "hadoop-aws-3.3.4.jar"),
    os.path.join(jar_dir, "aws-java-sdk-bundle-1.12.621.jar")
])

spark = SparkSession.builder \
    .appName("YouTube Data Transformation") \
    .config("spark.jars", jars) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Now load the data
raw_df = spark.read.json(f'{S3_BUCKET}/{raw_key}')
print('raw_df schema : ')
raw_df.printSchema()
print('first 5 raw records : \n')
raw_df.show(5, truncate=False)

#converting publish_time column to timestamp

clean_df = raw_df.select("channel_title", to_timestamp(col("publish_time")).alias("publish_time"),
                         "title", "video_id")

#checking for unqiue identier - video_id if it has any duplicates

def clean_and_deduplicate(df, column1, column2) :
    window_spec = Window.partitionBy(column1).orderBy(column2)
    clean_rank_df = df.withColumn("rnk", row_number().over(window_spec))
    # Check for duplicates
    duplicate_check_df = clean_rank_df \
        .filter(col("rnk") > 1) \
        .select("channel_title", "publish_time", "title", "video_id")
    # Count number of duplicates
    duplicate_count = duplicate_check_df.count()
    if duplicate_count > 0:
        print(f"✅ Found {duplicate_count} duplicates. Removing them.")
        deduped_df = clean_rank_df.filter(col("rnk") == 1).drop("rnk")
    else:
        print("✅ No duplicates found. Proceeding without changes.")
        deduped_df = clean_df
    return deduped_df


deduped_df = clean_and_deduplicate(clean_df,"video_id","publish_time")

#Removing emojis from ttitle column
def remove_emojis(text):
    return re.sub(r'[^\x00-\x7F]+','', text)

remove_emojis_udf = udf(remove_emojis, StringType())
clean_df = deduped_df.withColumn("title", remove_emojis_udf("title"))

#cleaning the title column
clean_df = clean_df.withColumn('title', regexp_replace("title", "$amp;","&"))
clean_df = clean_df.withColumn('title', regexp_replace("title", "&#39;;","'"))
clean_df = clean_df.withColumn('title', trim(col("title")))

#creating binary features
clean_df = clean_df.withColumn('is_ai_related', instr(lower("title"), "ai") > 0)
clean_df = clean_df.withColumn('is_music_related', instr(lower("title"), "music") > 0)
clean_df = clean_df.withColumn("is_google_related", instr(lower("title"), "google") > 0)
clean_df = clean_df.withColumn("is_tutorial_related", instr(lower("title"), "how") > 0)
clean_df = clean_df.withColumn("is_news_related", (instr(lower("title"), "breaking") > 0) |  (instr(lower("title"), "news") > 0) |
                               (instr(lower("title"), "update") > 0) |(instr(lower("title"), "world") > 0) | (instr(lower("title"), "report") > 0))


#computing the title length
clean_df = clean_df.withColumn("title_length", length("title"))

#categorizing the content so that we can get top contents
clean_df = clean_df.withColumn(
    "content_category",
    when(col("is_music_related") == True, "Music")
    .when(col("is_ai_related") == True, "AI")
    .when(col("is_google_related") == True, "Google Cloud")
    .when(col("is_tutorial_related") == True, "Tutorial")
    .when(col("is_news_related") == True, "News")
    .otherwise("Other")
)

#To understand how fresh is the video
clean_df = clean_df.withColumn("days_since_published", datediff(current_timestamp(), col("publish_time"))).withColumn('publish_date', to_date(col("publish_time")))


print("After all cleaning and transforming the data the output is : \n")
clean_df.show(10, truncate=False)

clean_df.printSchema()

clean_df.write.mode('overwrite').partitionBy('publish_date').parquet(f'{S3_BUCKET}/{PROCESSED_PREFIX}')





