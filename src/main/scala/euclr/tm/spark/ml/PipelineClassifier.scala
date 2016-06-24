package euclr.tm.spark.ml

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object PipelineClassifier {

    def readLabeledCorpus(sqlContext: SQLContext, labelToClassify: String, labeledCsv: String) = {
        import sqlContext.implicits._

        sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(labeledCsv)
            .map { row =>
                val label = row.getAs[String](labelToClassify)
                val doc = row.getAs[String]("TEXT")
                (label, transformText(doc))
            }
            .toDF("category", "text")
    }

    def readUnlabeledCorpus(sqlContext: SQLContext, unlabeledCsv: String) = {
        import sqlContext.implicits._

        sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(unlabeledCsv)
            .map { row =>
                val isin = row.getAs[String]("ISIN")
                val text = row.getAs[String]("TEXT")
                (isin, transformText(text))
            }
            .toDF("isin", "text")
    }

    def transformText(text: String) = text.replaceAll("\\W+", " ")

    def main(args: Array[String]) {

        val cli = args.toSeq.map { a =>
            val Array(k, v) = a.split("=")
            k -> v
        }.toMap[String, String]

        val labelToClassify = cli("labelToClassify")
        val labeledCsv = cli("labeledCsv")
        val unlabeledCsv = cli("unlabeledCsv")
        val outputPrefix = cli("outputPrefix")
        val w2vLocation = cli("w2vLocation")

        val formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
        val now = LocalDateTime.now().format(formatter)

        val outputFolder = s"$outputPrefix/$labelToClassify/$now"

        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val labeledCorpus = readLabeledCorpus(sqlContext, labelToClassify, labeledCsv)

        val indexer = new StringIndexer()
            .setInputCol("category")
            .setOutputCol("label")
            .fit(labeledCorpus)

        val tokenizer = new Tokenizer()
            .setInputCol("text")
            .setOutputCol("tokens")

        val stopWordsRemover = new StopWordsRemover()
            .setInputCol("tokens")
            .setOutputCol("words")
            .setStopWords(StopWords.stopWords)
            .setCaseSensitive(false)

        val word2Vec = Word2VecModel.load(w2vLocation)

        val randomForest = new RandomForestClassifier()
            .setLabelCol("label")
            .setFeaturesCol(word2Vec.getOutputCol)
            .setPredictionCol("prediction")

        val converter = new IndexToString()
            .setInputCol("prediction")
            .setOutputCol("predictedCategory")
            .setLabels(indexer.labels)

        val pipeline = new Pipeline()
            .setStages(Array(indexer, tokenizer, stopWordsRemover, word2Vec, randomForest, converter))

        val paramGrid = new ParamGridBuilder()
            .addGrid(word2Vec.windowSize, Array(3, 5, 9))
            .addGrid(randomForest.numTrees, Array(100, 200))
            .build()

        val mcv = new MulticlassClassificationEvaluator()

        val cv = new CrossValidator()
            .setEstimator(pipeline)
            .setEvaluator(mcv)
            .setEstimatorParamMaps(paramGrid)
            .setNumFolds(3)

        val cvModel = cv.fit(labeledCorpus)

        val unlabeledCorpus = readUnlabeledCorpus(sqlContext, unlabeledCsv)

        val predictions = cvModel.transform(unlabeledCorpus)
            .select("isin", "predictedCategory")
            .map { case Row(isin: String, prediction: String) =>
                s""""$isin","$prediction""""
            }

        // display a sample
        predictions.take(5).foreach(println)

        // cannot save rf
        // pipeline.save(s"$outputFolder/pipeline")
        // cvModel.save(s"$outputFolder/model")

        predictions.saveAsTextFile(s"$outputFolder/predictions")

    }
}
