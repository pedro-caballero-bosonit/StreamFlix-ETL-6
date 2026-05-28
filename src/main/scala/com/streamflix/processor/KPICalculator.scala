package com.streamflix.processor

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import scala.util.{Try, Success, Failure}

object KPICalculator {
  private val logger = Logger.getLogger(getClass.getName)
    //remplaza el println para escribir mensajes con nivel y contexto
  def topGenres(enrichedDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    // enrichedDF → viene del Enricher con columnas userId, movieId, genres, etc.
    // devuelve   → genre | total_hours ordenado de mayor a menor
    logger.info("Calculando KPI: Top géneros por horas")
      //"10:25:33 INFO  [KPICalculator] Calculando KPI: Top géneros por horas"
    Try{ //si falla va a Failure

      val genresDF = enrichedDF
        .filter(col("timestamp").startsWith("2026"))
        .withColumn("genre", explode(split(col("genres"), ",")))
        .groupBy("genre")
        .agg(sum("durationWatched").alias("total_hours"))
        .orderBy(col("total_hours").desc)

      logger.info(s"Géneros distintos encontrados: ${genresDF.count()}")
      genresDF
    } match { //evalua el resultado del Try y actúa
      case Success(df) =>
        logger.info("KPI Top géneros completado")
        df
      case Failure(ex) =>
        logger.info(s"Error calculando géneros: ${ex.getMessage}")
        throw ex
        //lo volvemos a lanzar para que el Main sepa que algo fallo y pueda detener el job
    }
  }

  def topBingeWatchers(enrichedDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    // enrichedDF → viene del Enricher
    // devuelve   → userId | binge_count ordenado de mayor a menor
    logger.info("Calculando KPI: Top Binge Watchers")

    Try{
      //1. defino la ventana
      val windowSpec = Window
        .partitionBy("userId") //divide los datos por usuario
        .orderBy("timestamp") //dentro de cada usuario ordena cronologicamente para lag()

      //2. calculo cuando termina cada pelicula
      val endTimeDF = enrichedDF.withColumn(
        "end_time_seconds",
        unix_timestamp(col("timestamp")) + col("durationWatched") * 60 //pasamos a segundos para sumarlo con el resultado de unix_timestamp
          //unix_timestamp() convierte un String de fecha a segundos desde el 1 enero 1970
      )
      //3. traigo el end_time de la pelicula anterior
      val prevEndTimeDF = endTimeDF.withColumn(
        "prev_end_time",
        lag("end_time_seconds", 1).over(windowSpec)
          //dentro de cada usuario calcula q fila atras, .over() le dice que en que ventana operar
      )

      //4. calculo la pausa real en minutos
      val diffDF = prevEndTimeDF.withColumn(
        "diff_minutes",
        (unix_timestamp(col("timestamp")) - col("prev_end_time")) / 60
        //para calcular los minutos entre el final de la pelicula anterior y el incio de la actual
      )

      //5. compruebo si es Binge Watching
      val bingeDF = diffDF.withColumn(
        "is_binge",
        when(
          col("diff_minutes") < 20 && col("diff_minutes") > 0,
          true
        ).otherwise(false)
      )

      //6. cuento los Binge Watching de cada usuario
      val topBingeDF = bingeDF
        .filter(col("is_binge") === true)
        .groupBy("userId")
        .agg(count("*").alias("binge_count"))
        .orderBy(col("binge_count").desc)

      logger.info(s"Top binge watchers calculado: ${topBingeDF.count()} usuarios")
      topBingeDF

    } match {
      case Success(df) =>
        logger.info("KPI Binge Watchers completado")
        df
      case Failure(ex) =>
        logger.info(s"Error calculando binge watchers: ${ex.getMessage}")
        throw ex
    }
  }
}
