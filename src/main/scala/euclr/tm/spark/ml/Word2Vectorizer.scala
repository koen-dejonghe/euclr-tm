package euclr.tm.spark.ml

import org.apache.spark.ml.feature._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Word2Vectorizer {

    def main(args: Array[String]) {

        val cli = args.map( _.split("=") match { case Array(k, v) => k -> v } ).toMap

        val inputDataFile = cli("input")
        val saveAs = cli("saveAs")

        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        import sqlContext.implicits._

        // see Mikolovs comments here: https://groups.google.com/forum/#!topic/word2vec-toolkit/TI-TQC-b53w

        val corpus = sc.textFile(inputDataFile)
            // remove all punctuation and non word characters
            .map(_.replaceAll("\\W+", " "))
            .toDF("text")

        val tokens = new RegexTokenizer()
            .setInputCol("text")
            .setOutputCol("tokens")
            .setToLowercase(false) // do not transform to lowercase
            .transform(corpus)

        // values taken from https://www.kaggle.com/c/word2vec-nlp-tutorial/details/part-2-word-vectors
        val word2Vec = new Word2Vec()
            .setInputCol("tokens")
            .setOutputCol("features")
            .setVectorSize(300)
            .setWindowSize(10)
            .setMinCount(30)
            .setNumPartitions(3)
            .fit(tokens)

        word2Vec.save(saveAs)

    }

}
