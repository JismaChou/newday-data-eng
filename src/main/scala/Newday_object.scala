import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, min}


object Newday_object {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("Newday").master("local").getOrCreate()


    val Schema = StructType(Array(
      StructField("MovieID", IntegerType, true),
      StructField("Title", StringType, true),
      StructField("Genres", StringType, true)))

    val Schema1 = StructType(Array(
      StructField("UserID", IntegerType, true),
      StructField("MovieID", StringType, true),
      StructField("Rating", StringType, true),
      StructField("Timestamp", StringType, true)))


    val movie_text = spark.read.textFile("./src/main/scala/ml-1m/movies.dat")
    val df = spark.read.option("delimiter", "::").schema(Schema).csv(movie_text)

    val rating_text = spark.read.textFile("./src/main/scala/ml-1m/ratings.dat")
    val df1 = spark.read.option("delimiter", "::").schema(Schema1).csv(rating_text)

    //val df = spark.read.option("header", "true").option("delimiter", "::").csv("src/main/scala/ml-1m/movies.dat")
    df.show(10)
    df1.show(10)

    val joindataframe = df.join(df1, df("MovieID") === df1("MovieID"), "inner")
    joindataframe.show()

    val dfnew = joindataframe.groupBy("Title").agg(
      max("Rating").as("Max Rating"),
      min("Rating").as("Min Rating"),
      avg("Rating").as("Average Rating")

    )
    dfnew.show()

    joindataframe.createOrReplaceTempView("df_temp_view")

    val dfnew1 = spark.sql("select * from (select UserID, Title, Rating, row_number() over (partition by UserID order by Rating desc) as col1 from df_temp_view)rnk where col1 <=3").drop("col1")
    dfnew1.show()

    df.write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ",")
      .json("./result/df.json")


    df1.write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ",")
      .json("./result/df1f.json")


    dfnew.write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ",")
      .json("./result/dfnew.json")


    dfnew1.write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ",")
      .json("./result/dfnew1.json")
  }

}
