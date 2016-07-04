package euclr.tm.spark.ml

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object PipelineClassifier {

    def main(args: Array[String]) {

        val cli = args.map( _.split("=") match { case Array(k, v) => k -> v } ).toMap
        val pipeline = cli("pipeline")
        val unlabeled = cli("unlabeled")
        val saveAs = cli("saveAs")

        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val corpus = sqlContext.read.parquet(unlabeled)
        val model = PipelineModel.load(pipeline)

        val predictions = model
            .transform(corpus)
            .select("ISIN", "predictedCategory")

        predictions.take(5).foreach(println)

        predictions.write
            .mode(SaveMode.Overwrite)
            .format("com.databricks.spark.csv")
            .option("header", "false")
            .option("quoteMode", "NON_NUMERIC")
            .save(saveAs)

    }
}
