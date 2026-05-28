package com.streamflix.processor

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.log4j.Logger
import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SaveMode

object Writer {

  private val logger = Logger.getLogger(getClass.getName)

  def saveParquet(df: DataFrame, outputPath: String)(implicit spark: SparkSession): Unit = {
    logger.info(s"Iniciando escritura en: $outputPath")

    Try{
      val columnasRequeridas = Seq("year", "country")
      columnasRequeridas.foreach { columna =>
        if (!df.columns.contains(columna)) {
          throw new IllegalArgumentException(s"Falta la columna requerida para particionar: $columna")
        }
      }

    logger.info(s"Columnas verificadas: ${df.columns.mkString(", ")}")
    df.write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "country")
      .parquet(outputPath)

    logger.info(s"Datos guardados correctamente en: $outputPath")

    } match {
      case Success(_) =>
        logger.info("Escritura completada exitosamente")
      case Failure(ex) =>
        logger.info(s"Error escribiendo Parquet: ${ex.getMessage}")
        throw ex
    }
  }
}