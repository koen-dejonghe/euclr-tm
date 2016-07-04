package euclr.tm.spark.ml

import euclr.tm.spark.ml.util.Helper
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, RegexTokenizer, StringIndexer, Word2VecModel}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object W2vRfClassifier {

    def main(args: Array[String]) {

        val cli = args.map( _.split("=") match { case Array(k, v) => k -> v } ).toMap

        val labelToClassify = cli("labelToClassify")
        val labeledCsv = cli("labeledCsv")
        val w2vLocation = cli("w2v")
        val evaluate = cli.getOrElse("evaluate", "true").toBoolean
        val saveAs = if (! evaluate) cli("saveAs") else ""

        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val labeledCorpus = Helper.readLabeledCorpus(sqlContext, labelToClassify, labeledCsv)

        val trainingPct = if (evaluate) 0.7 else 1.0
        val testPct = 1.0 - trainingPct

        val Array(trainingSet, testSet) = labeledCorpus.randomSplit(Array(trainingPct, testPct), seed = 1234L)
        trainingSet.cache()

        val indexer = new StringIndexer()
            .setInputCol("category")
            .setOutputCol("label")
            .fit(labeledCorpus)

        val tk = new RegexTokenizer()
            .setInputCol("text")
            .setOutputCol("tokens")
            .setToLowercase(false) // do not transform to lowercase

        val w2v = Word2VecModel.load(w2vLocation)
            .setInputCol("tokens")
            .setOutputCol("features")

        val rf = new RandomForestClassifier()
            .setLabelCol("label")
            .setFeaturesCol("features")
            .setNumTrees(20) // default: 20
            .setMaxDepth(30) // default: 5, max: 30

        val converter = new IndexToString()
            .setInputCol("prediction")
            .setOutputCol("predictedCategory")
            .setLabels(indexer.labels)

        val pipeline = new Pipeline()
            .setStages(Array(indexer, tk, w2v, rf, converter))

        val model = pipeline.fit(trainingSet)

        if (evaluate) {
            val predictions = model.transform(testSet)

            val mcv = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")

            val accuracy = mcv.evaluate(predictions)
            println(s"accuracy: $accuracy")
        }
        else {
            model.write.overwrite.save(saveAs)
        }

    }

}
