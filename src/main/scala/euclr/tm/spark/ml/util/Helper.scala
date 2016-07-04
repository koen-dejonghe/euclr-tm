package euclr.tm.spark.ml.util

import org.apache.spark.sql.SQLContext

object Helper {

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

    def readCorpus(sqlContext: SQLContext, csv: String) = {
        sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(csv)
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

}
