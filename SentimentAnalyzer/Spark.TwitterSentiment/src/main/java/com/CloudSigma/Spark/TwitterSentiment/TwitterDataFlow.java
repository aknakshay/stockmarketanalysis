package com.CloudSigma.Spark.TwitterSentiment;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;

import com.CloudSigma.Spark.TwitterSentiment.StanfordSentiment;

public class TwitterDataFlow {

	public static void main(String[] args) {

		if (args.length != 2) {
			System.out.println("Incorrect number of arguments! \n");
			System.out.println(
					"Usage: Input Output \n " + "Input: Location where the partitioned data needs to be read from"
							+ "Output: Where the final result needs to be stored");
			System.exit(1);
		}

		SparkSession spark = SparkSession.builder().appName("Sentiment Analyzer").getOrCreate();

		spark.sparkContext().hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true");

		String inputPath = args[0] + "/*/*";

		Dataset<Row> data = spark.read().json(inputPath);

		StanfordSentiment ss = new StanfordSentiment();

		spark.sqlContext().udf().register("Sentiment", (String s) -> ss.GetSentiment(s), DataTypes.DoubleType);

		List<String> list = Arrays.asList("apple", "google", "tesla", "infosys", "tcs", "oracle", "microsoft",
				"facebook");

		String query;
		String outPath;

		Dataset<Row> result;

		for (String company : list) {

			outPath = args[1] + "/" + company;

			data.createOrReplaceTempView("complete");

			// tmp1 extracts TimeStamp, partitionBy (Date), tweet text, tweet text in lower
			// case and followers_count of the user tweeting
			Dataset<Row> tmp1 = spark.sql(
						"select concat(substr(created_at,5,6), substr(created_at,26,5),' ',substr(created_at,12,6),'00') as timestamp,substr(created_at,5,6) as partitionBy,text,lower(text) as main_text,user.followers_count as followers from complete");

			tmp1.createOrReplaceTempView("tmp");

			// Filtering tweets having certain company names in it
			Dataset<Row> tmp2 = spark.sql("select * from tmp where main_text regexp '(" + company + ")'");

			// Creates a view named twitter
			tmp2.createOrReplaceTempView("twitter");

			// tmp3 contains the entire selected data along with the Sentiment value of the
			// tweets
			Dataset<Row> tmp3 = spark.sql("select  *, Sentiment(text) as seVal from twitter");

			tmp3.persist(StorageLevel.MEMORY_AND_DISK());

			// Creating another view
			tmp3.createOrReplaceTempView("dataSe");

			Dataset<Row> net = spark.sql(
					"select  *,followers*seVal as NetSentiment from dataSe");

			// Creating a final view to save the data
			net.createOrReplaceTempView("final");

			// Averaging the Sentiment Values per minute by grouping the data onto it
			query = "select timestamp,partitionBy,AVG(NetSentiment) from final group by timestamp,partitionBy";

			// Saving the result in result dataset
			result = spark.sql(query);

			// Writing the result onto the disk
			result.write().partitionBy("partitionBy").json(outPath);
		}
	}
}
