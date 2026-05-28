package com.streamflix.processor

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import scala.util.{Try, Success, Failure}

object DataLoader {

  private val logger = Logger.getLogger(getClass.getName)

  private val movieSchema = StructType(Array(
    StructField("id",LongType, nullable = false),
    StructField("title",StringType, nullable = false),
    StructField("genres",StringType, nullable = true),
    StructField("subscription_price",StringType, nullable = true),
    StructField("release_date",StringType, nullable = true),
    StructField("country",StringType, nullable = true)
  ))

  def loadLogs(path : String)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Cargando logs desde: $path")
    Try(spark.read.text(path)) match {
      case Success(df) =>
        logger.info(s"Logs cargados correctamente")
        df
      case Failure(ex) =>
        logger.error(s"Error cargando logs: ${ex.getMessage}")
        throw ex
    }
  }
  def loadMovies(path: String)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Cargando movies desde: $path")
    Try(
      spark.read
        .option("header", "true")
        .schema(movieSchema)
        .option("mode", "DROPMALFORMED")
        .csv(path)
    ) match {
      case Success(df) =>
        logger.info(s"Movies cargadas correctamente")
        df
      case Failure(ex) =>
        logger.error(s"Error cargando movies: ${ex.getMessage}")
        throw ex
    }
  }
}