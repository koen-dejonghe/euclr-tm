package euclr.tm.spark.ml

import java.text.SimpleDateFormat

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object TfIdfNBClassifier {

    def main(args: Array[String]) {

        val cli = args.map( _.split("=") match { case Array(k, v) => k -> v } ).toMap

        val labelToClassify = cli("labelToClassify")
        val labeledLemmas = cli("labeledLemmas")
        val evaluate = cli.getOrElse("evaluate", "true").toBoolean

        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val lemmaCorpus = sqlContext.read.parquet(labeledLemmas)

        val trainingPct = if (evaluate) 0.7 else 1.0
        val testPct = 1.0 - trainingPct

        val Array(trainingSet, testSet) = lemmaCorpus.randomSplit(Array(trainingPct, testPct), seed = 1234L)
        trainingSet.cache()

        val indexer = new StringIndexer()
            .setInputCol(labelToClassify)
            .setOutputCol("label")
            .fit(lemmaCorpus)

        val stopWordsRemover = new StopWordsRemover()
            .setInputCol("LEMMAS")
            .setOutputCol("words")
            .setStopWords(util.StopWords.stopWords)
            .setCaseSensitive(false)

        val ngram = new NGram()
            .setInputCol("words")
            .setOutputCol("ngrams")
            .setN(2)

        val tf = new HashingTF()
            .setInputCol("ngrams")
            .setOutputCol("tf")
            .setNumFeatures(10000) // for ISSUER_NAME

        val idf = new IDF()
            .setInputCol("tf")
            .setOutputCol("features")

        val nb = new NaiveBayes()
            .setLabelCol("label")
            .setFeaturesCol("features")
            .setPredictionCol("prediction")

        val converter = new IndexToString()
            .setInputCol("prediction")
            .setOutputCol("predictedCategory")
            .setLabels(indexer.labels)

        val pipeline = new Pipeline()
            .setStages(Array(indexer, stopWordsRemover, ngram, tf, idf, nb, converter))

        val model = pipeline.fit(trainingSet)

        model.parent.extractParamMap.toSeq.foreach { pp =>
            println (s"${pp.param.name} = ${pp.value}")
        }

        if (evaluate) {
            val predictions = model.transform(testSet)

            val mcv = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")

            val accuracy = mcv.evaluate(predictions)
            println(s"accuracy: $accuracy")
        }
        else {
            val saveAs = cli("saveAs")
            model.write.overwrite.save(saveAs)
        }

    }
}
