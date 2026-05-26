package com.streamflix.processor

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.{Try, Success, Failure}

object Enricher {
  private val logger = Logger.getLogger(getClass.getName)

  def enrich(logsDF: DataFrame, moviesDF: DataFrame)
            (implicit spark: SparkSession): DataFrame = {
    logger.info("Enriquecimiento de datos")

    Try{
      val enrichedDF = logsDF.join(
        broadcast(moviesDF),
        logsDF("moviesId") === moviesDF("id"),
        "inner"
      )
      logger.info(s"Join completado: ${enrichedDF.count()} registros")

      val resultDF = enrichedDF.select(
        col("userId"),
        col("movieId"),
        col("durationWatched"),
        col("timestamp"),
        col("title"),
        col("genres"),
        col("year"),
        col("country")
      )
      logger.info(s"Columnas seleccionadas: ${resultDF.columns.mkString(", ")}")
      resultDF
    } match {
      case Success(df) =>
        logger.info("Enriquecimiento completo")
        df
      case Failure(ex) =>
        logger.error((s"Error de enriquecimiento: ${ex.getMessage}"))
        throw ex
    }
  }
}