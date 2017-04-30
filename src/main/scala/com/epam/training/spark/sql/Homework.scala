package com.epam.training.spark.sql

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Homework {
  val DELIMITER = ";"
  val RAW_BUDAPEST_DATA = "data/budapest_daily_1901-2010.csv"
  val OUTPUT_DUR = "output"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("EPAM BigData training Spark SQL homework")
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.shuffle.partitions", "10")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    processData(sqlContext)

    sc.stop()

  }

  def processData(sqlContext: HiveContext): Unit = {

    /**
      * Task 1
      * Read csv data with DataSource API from provided file
      * Hint: schema is in the Constants object
      */
    val climateDataFrame: DataFrame = readCsvData(sqlContext, Homework.RAW_BUDAPEST_DATA)

    /**
      * Task 2
      * Find errors or missing values in the data
      * Hint: try to use udf for the null check
      */
    val errors: Array[Row] = findErrors(climateDataFrame)
    println(errors)

    /**
      * Task 3
      * List average temperature for a given day in every year
      */
    val averageTemeperatureDataFrame: DataFrame = averageTemperature(climateDataFrame, 1, 2)

    /**
      * Task 4
      * Predict temperature based on mean temperature for every year including 1 day before and after
      * For the given month 1 and day 2 (2nd January) include days 1st January and 3rd January in the calculation
      * Hint: if the dataframe contains a single row with a single double value you can get the double like this "df.first().getDouble(0)"
      */
    val predictedTemperature: Double = predictTemperature(climateDataFrame, 1, 2)
    println(s"Predicted temperature: $predictedTemperature")

  }

  def readCsvData(sqlContext: HiveContext, rawDataPath: String): DataFrame = {
    val options = Map("header" -> "true", "DELIMITER" -> ";")
    sqlContext.read.options(options).schema(Constants.CLIMATE_TYPE).csv(rawDataPath)
  }

  def findErrors(climateDataFrame: DataFrame): Array[Row] = {
    val exprs = climateDataFrame.columns.map(col).map(nullCount)
    climateDataFrame.agg(exprs.head, exprs.tail: _*).collect()
  }

  private def nullCount(column: Column): Column =
    sum(when(column.isNull, 1).otherwise(0))

  def averageTemperature(climateDataFrame: DataFrame, monthNumber: Int, dayOfMonth: Int): DataFrame = {
    import climateDataFrame.sqlContext.implicits._
    val condition = isMonthAndDayEqual($"observation_date", monthNumber, dayOfMonth)
    climateDataFrame.select($"mean_temperature").where(condition)
  }

  def predictTemperature(climateDataFrame: DataFrame, monthNumber: Int, dayOfMonth: Int): Double = {
    val condition = isMonthAndDayEqualOrAdjacent(climateDataFrame("observation_date"), monthNumber, dayOfMonth)
    climateDataFrame.groupBy(condition).avg("mean_temperature").toDF().first().getDouble(1)
  }

  private def isMonthAndDayEqual(date: Column, monthNumber: Int, dayOfMonth: Int): Column =
    month(date).equalTo(monthNumber).and(dayofmonth(date).equalTo(dayOfMonth))

  private def isMonthAndDayEqualOrAdjacent(date: Column, monthNumber: Int, dayOfMonth: Int): Column = {
    val previousDate = date_sub(date, 1)
    val nextDate = date_add(date, 1)
    isMonthAndDayEqual(date, monthNumber, dayOfMonth)
      .or(isMonthAndDayEqual(previousDate, monthNumber, dayOfMonth))
      .or(isMonthAndDayEqual(nextDate, monthNumber, dayOfMonth))
  }

}


