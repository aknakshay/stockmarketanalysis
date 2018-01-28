import org.apache.spark.sql.SparkSession
import Methods._

object Process {

  def main(args : Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Sentiment Analyzer").getOrCreate()

    spark.sparkContext.hadoopConfiguration.get("mapreduce.input.fileinputformat.input.dir.recursive")
    spark.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val data = spark.read.json("/home/nikita/Documents/backupHDFS/*/*")

    data.createOrReplaceTempView("twitter")

    val tweets = spark.sql("SELECT * FROM TWITTER")
    
    val sentiments = tweets.map(tweet => Sentiments(SpellCheck(tweet.getAs[String]("text"))))
    
    sentiments.show()
  }

}
