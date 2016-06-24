package euclr.tm.spark.ml

import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer, Word2Vec}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Word2Vectorizer {

    def main(args: Array[String]) {

        val cli = args.toSeq.map { a =>
            val Array(k, v) = a.split("=")
            k -> v
        }.toMap[String, String]

        val inputDataFile = cli("input")
        val saveAs = cli("saveAs")

        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        import sqlContext.implicits._

        val corpus = sc.textFile(inputDataFile).map(_.replaceAll("\\W+", " ")).toDF("text")

        val tokens = new Tokenizer()
            .setInputCol("text")
            .setOutputCol("tokens")
            .transform(corpus)

        val cleaned = new StopWordsRemover()
            .setInputCol("tokens")
            .setOutputCol("words")
            .setStopWords(StopWords.stopWords)
            .setCaseSensitive(false)
            .transform(tokens)

        val word2Vec = new Word2Vec()
            .setInputCol("words")
            .setOutputCol("features")
            .setVectorSize(100)
            .setNumPartitions(8)
            .fit(cleaned)

        word2Vec.save(saveAs)

    }

}
