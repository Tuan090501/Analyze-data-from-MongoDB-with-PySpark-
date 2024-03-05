from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
from pyspark.sql.types import *
from extractFunction import extract_language, extract_domain
import re
import findspark
findspark.init()
if __name__ == "__main__":
    
    spark = SparkSession \
            .builder \
            .master('local[*]') \
            .appName('DEP3') \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .config('spark.mongodb.input.uri', 'mongodb://localhost:27017/ASM3.questions') \
            .config("spark.memory.offHeap.enabled","true") \
            .config("spark.memory.offHeap.size","10g") \
            .getOrCreate()
                    
    #Read data from the database and normalize the data.
    df1 = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .load()

    questions_df = df1.withColumn('OwnerUserId', expr("case when OwnerUserId == 'NA' then null else cast(OwnerUserId as int) end")) \
        .withColumn('CreationDate', col('CreationDate').cast(DateType())) \
        .withColumn('ClosedDate', expr("case when ClosedDate == 'NA' then null else cast(CreationDate as date) end"))
    print('ANSWERS SCHEMA')
    questions_df.printSchema()
    
    
    df2 = spark.read \
      .format('com.mongodb.spark.sql.DefaultSource') \
      .option("uri", "mongodb://127.0.0.1/ASM3.answers") \
      .load()
  
    answers_df = df2.withColumn('OwnerUserId', col('OwnerUserId').cast(IntegerType())) \
      .withColumn('CreationDate', col('CreationDate').cast(DateType()))
    print('ANSWERS SCHEMA')
    answers_df.printSchema()
    
    #Counting Occurrences of Programming Languages
    #extract_language_udf = udf(extract_language,StringType())
   
    #languages_df = questions_df.withColumn('Programing_Language', extract_language_udf(col('Body'))) \
    #                .groupBy('Programing_Language') \
    #                .agg(count('Programing_Language').alias('Count')) */
                    
    #languages_df.show() 
    
    #Find the most used domains in questions
    questions_df1 = questions_df.repartition(10)
    extract_domains_udf = udf(extract_domain, ArrayType(StringType()))
    domains_df = questions_df1.withColumn('Body_extract', extract_domains_udf(col('Body'))) \
        .withColumn('Domain', explode(col('Body_extract'))) \
        .groupBy('Domain') \
        .agg(count('Domain').alias('Count')) \
        .sort(col('Count').desc())
    domains_df.show()
    
    #User's total score by day
    questions_df.createOrReplaceTempView("total_core")
    total_score_sql = spark.sql("""select OwnerUserId, CreationDate, sum(Score) as TotalScore from total_core where OwnerUserId is not null group by OwnerUserId,CreationDate order by OwnerUserId,CreationDate """) 
    total_score_sql.show()    
    
    #Calculate the total number of points a User achieves in a period of time
    START = '01-01-2008'
    END = '01-01-2009'
    
    formatted_start = datetime.strptime(START, '%d-%m-%Y').strftime('%Y-%m-%d')
    formatted_end = datetime.strptime(END, '%d-%m-%Y').strftime('%Y-%m-%d')
    
    total_score_in_range = questions_df.filter((col('CreationDate') >= formatted_start) & (col('CreationDate') >= formatted_end)) \
                            .dropna(subset=['OwnerUserId']) \
                            .groupBy('OwnerUserId') \
                            .agg(sum('Score').alias('TotalScore')) \
                            .orderBy('OwnerUserId')
    total_score_in_range.show()

    #Find questions with multiple answers
    spark.sql("Create database if not exists Bucketed_DB")
    spark.sql("USE Bucketed_DB")
    questions_df.coalesce(1).write \
        .bucketBy(20, 'Id') \
        .mode('overwrite') \
        .saveAsTable('Bucketed_DB.myQuestions')

    answers_df.coalesce(1).write \
        .bucketBy(20, 'ParentId') \
        .mode('overwrite') \
        .saveAsTable('Bucketed_DB.myAnswers')
    
    df3 = spark.read.table('Bucketed_DB.myQuestions')
    df4 = spark.read.table('Bucketed_DB.myAnswers')
    
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
    
    join_expr = df3.Id == df4.ParentId
    df4.withColumnRenamed('Id', 'Answer ID') \
        .join(df3, join_expr, 'inner') \
        .select(col('Id').alias('Question ID'), 'Answer ID') \
        .groupBy('Question ID') \
        .agg(count('Answer ID').alias('Total Answers')) \
        .orderBy(col('Question ID').asc()) \
        .filter(col('Total Answers') > 5) \
        .show()
    
    
    
    
    
    
    
    