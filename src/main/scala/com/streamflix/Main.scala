package com.streamflix

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import com.streamflix.processor._
import scala.util.{Try, Success, Failure}

object Main {

  private val logger = Logger.getLogger(getClass.getName)
  // logger identificado como "com.streamflix.Main"

  def main(args: Array[String]): Unit = {

    //1. Verifico que se pasaron los argumentos necesarios
    if (args.length < 3) {
      // si faltan argumentos → mostramos cómo usar el job y salimos
      println("Uso: sbt run <logsPath> <moviesPath> <outputPath>")
      println("Ejemplo:")
      println("  sbt \"run src/main/resources/data/server_logs.txt \\")
      println("           src/main/resources/data/movies_metadata.csv \\")
      println("           src/main/resources/output/analytics_warehouse\"")
      System.exit(1)
      // System.exit(1) → termina con código de error
    }

    //2. Recogemos los argumentos
    val logsPath   = args(0)
    // ruta del archivo de logs → server_logs.txt
    val moviesPath = args(1)
    // ruta del catálogo de películas → movies_metadata.csv
    val outputPath = args(2)
    // ruta donde guardar el Parquet → output/analytics_warehouse

    logger.info("=== StreamFlix ETL Job iniciado ===")
    logger.info(s"Logs:   $logsPath")
    logger.info(s"Movies: $moviesPath")
    logger.info(s"Output: $outputPath")

    //3. Creo la SparkSession
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("StreamFlix-ETL")
      .master("local[*]")
      // local[*] → ejecuta en local usando todos los cores disponibles
      // en producción esto vendría como argumento de spark-submit
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    // silenciamos los logs internos de Spark

    //4. Ejecuto el flujo ETL completo
    Try {

      // ETL 1: Leer datos crudos
      logger.info("--- ETL Paso 1: Cargando datos ---")
      val rawLogsDF   = DataLoader.loadLogs(logsPath)
      val rawMoviesDF = DataLoader.loadMovies(moviesPath)

      // ETL 2: Limpiar y tipar
      logger.info("--- ETL Paso 2: Limpiando datos ---")
      val logsDF   = Cleaner.cleanLogs(rawLogsDF)
      val moviesDF = Cleaner.cleanMovies(rawMoviesDF)

      // ETL 3: Enriquecer con metadata
      logger.info("--- ETL Paso 3: Enriqueciendo datos ---")
      val enrichedDF = Enricher.enrich(logsDF, moviesDF)

      // ETL 4: Calcular KPIs
      logger.info("--- ETL Paso 4: Calculando KPIs ---")
      val topGenresDF      = KPICalculator.topGenres(enrichedDF)
      val topBingeDF       = KPICalculator.topBingeWatchers(enrichedDF)

      logger.info("--- Top 10 Géneros ---")
      topGenresDF.show(10)

      logger.info("--- Top 10 Binge Watchers ---")
      topBingeDF.show(10)

      // ETL 5: Guardar en Parquet
      logger.info("--- ETL Paso 5: Guardando resultados ---")
      Writer.saveParquet(enrichedDF, outputPath)

    } match {
      case Success(_) =>
        logger.info("=== StreamFlix ETL Job completado exitosamente ===")
      case Failure(ex) =>
        logger.error(s"=== StreamFlix ETL Job fallido: ${ex.getMessage} ===")
        spark.stop()
        System.exit(1)
      // System.exit(1) → indica al sistema que el job falló
    }

    //5. Cerramos la SparkSession
    spark.stop()
    // libera todos los recursos de Spark
    // siempre al final, pase lo que pase
  }
}

