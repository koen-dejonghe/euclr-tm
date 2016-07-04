package euclr.tm.spark.ml

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.util.CoreMap
import euclr.tm.spark.ml.util.Helper
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/*
Transforms a corpus of continuous text into a corpus of lemmas, using the Stanford Core NLP pipeline.
Main reason this is implemented as a stand-alone object is that StanfordCoreNLP is not Serializable.
Otherwise this could have been implemented as a Transformer.
 */
object Lemmatizer {

    def isOnlyLetters(str: String) = str.forall(c => Character.isLetter(c))

    def main(args: Array[String]): Unit = {

        val cli = args.map( _.split("=") match { case Array(k, v) => k -> v } ).toMap

        val conf = new SparkConf().setAppName(this.getClass.getName)
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val corpus: DataFrame = Helper.readCorpus(sqlContext, cli("input"))

        val lemmaRDD = corpus.mapPartitions { it =>

            val props = new Properties()
            props.put("annotators", "tokenize, ssplit, pos, lemma")
            val pipeline = new StanfordCoreNLP(props)

            it.map { (row: Row) =>
                val text = row.getAs[String]("TEXT")
                val doc = new Annotation(text)
                pipeline.annotate(doc)

                val lemmas = new ArrayBuffer[String]()
                val sentences: java.util.List[CoreMap] = doc.get(classOf[SentencesAnnotation])

                for (sentence <- sentences;
                     token <- sentence.get(classOf[TokensAnnotation])) {
                    val lemma = token.get(classOf[LemmaAnnotation])
                    if (lemma.length > 2 && isOnlyLetters(lemma)) {
                        lemmas += lemma.toLowerCase
                    }
                }

                Row.fromSeq(row.toSeq :+ lemmas)
            }
        }

        // add the lemmas field to the original schema
        val lemmasField = StructField("LEMMAS", DataTypes.createArrayType(StringType), nullable = true)
        val schema = StructType( corpus.schema.fields :+ lemmasField)

        // create a data frame with the new schema
        val lemmaCorpus = sqlContext.createDataFrame(lemmaRDD, schema)

        // save the data frame
        lemmaCorpus.write.mode(SaveMode.Overwrite).save(cli("saveAs"))

    }

}
