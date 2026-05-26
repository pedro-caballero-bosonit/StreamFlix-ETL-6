package com.streamflix.processor

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.{Try, Success, Failure}

object Cleaner {
  private val logger = Logger.getLogger(getClass.getName)

  def cleanLogs(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    logger.info("Limpieza de logs")

    Try{
      //1. Nos quedamos con las lineas correctas, descartamos [ERROR] y [WARN] corruptas
      val infoDF = df.filter(col("value").startsWith("[INFO]"))

      //2. Recibimos [INFO] 2025-11-13 18:00:00|User:1001|Play:Movie_1|Dur:40 y extrameos cada cosa
      val splitDF = infoDF
        .withColumn("timestamp",
          substring(col("value"), 8, 19))
        .withColumn("userId",
          split(col("value"), "\\|")(1))
        .withColumn("movieId",
          split(col("value"), "\\|")(2))
        .withColumn("durationWatched",
          split(col("value"), "\\|")(3))

      //3. Limpieza
      val cleanedDF = splitDF
        .withColumn("userId",
          split(col("userId"), ":")(1))
        .withColumn("movieId",
          split(col("movieId"), "_")(1))
        .withColumn("durationWatched",
          split(col("durationWatched"), ":")(1))

      //4. Casteo y Limpieza
      val logsDF = cleanedDF
        .withColumn("userId",
          col("userId").cast("long"))
        .withColumn("movieId",
          col("movieId").cast("long"))
        .withColumn("durationWatched",
          col("durationWatched").cast("long"))
        .drop("value") //elimino la columna original
        .na.drop() //elimino las filas donde la columna sea null

      logger.info(s"Logs limpios: ${logsDF.count()} registros válidos")
      logsDF
    } match {
      case Succes(df) =>
        logger.info("Limpieza de logs completada")
        df //DataFrame limpio
      case Failure(ex) =>
        logger.error(s"Error limpiando logs: ${ex.getMessage}")
        throw ex
    }
  }
  def cleanMovies(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    logger.info("Limpieza de movies")

    Try{
      val moviesDF = df
        .withColumn("id",
          col("id").cast("long"))
        .withColumn("subscription_price",
          regexp_replace(col("subscription_price"), "\\$", "")
          .cast("double"))
        .withColumn("genres",
          regexp_replace(col("genres"), "\\|", ","))
        .withColumn("release_date",
          regexp_replace(col("release_date"), "invalid-date", "ERROR"))
        .withColumn("year",
          split(col("release_date"), "-")(0))
        .na.fill("Unknown", Seq("genres"))

      logger.info(s"Movies limpias: ${moviesDF.count()} registros válidos")
      moviesDF
    } match {
      case Success(df) =>
        logger.info("Limpieza de movies")
        df
      case Failure(ex) =>
        logger.error(s"Error de limpieza: ${ex.getMessage}")
        throw ex
    }
  }
}