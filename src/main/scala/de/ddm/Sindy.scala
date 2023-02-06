package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scala.collection.mutable

object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    // Create a HashSet to store the column names that only have a single value
    val valuesWithEmptySet = mutable.HashSet[String]()
    // Create a HashMap to store the column names and the set of values they appear with
    val candidates = mutable.HashMap[String, Set[String]]()

    inputs.foreach(input => {
      // Read the data into a dataframe
      val df = readData(input, spark)
      // Get the list of column names in the dataframe
      val columns = df.columns

      // Loop through each row
      df.collect().foreach(row => {
        // Loop through each column in the row
        for (i <- 0 until columns.length) {
          // Get the value of the current column in the current row
          val value = row.getString(i)
          // Get the name of the current column
          val colName = columns(i)

          // If the column name has already been seen, add the current value to the set of values it appears with
          if (candidates.contains(colName)) {
            candidates(colName) += value
          } else {
            // If the column name hasn't been seen before, create a new entry in the HashMap with the column name and the current value
            candidates(colName) = Set(value)
          }
        }
      })
    })

    // Loop through each column name and its set of values
    for ((colName, values) <- candidates) {
      // If the column only has a single value, add it to the HashSet of column names with single values
      if (values.size == 1) {
        valuesWithEmptySet.add(colName)
      } else {
        // If the column has more than one value, intersect its set of values with the sets of values for all other columns
        for (candidate <- candidates.keys) {
          if (candidate != colName) {
            candidates(candidate) = candidates(candidate).intersect(values)
          }
        }
      }
    }

    // Filter out any entries in the HashMap with an empty set of values or with a column name that only has a single value
    val finalResults = candidates.filterNot(kv => kv._2.isEmpty || valuesWithEmptySet.contains(kv._1))

    // Sort the final results by the column name
    finalResults.toSeq.sortBy(_._1).foreach(result => {
      println(s"${result._1} < ${result._2.toSeq.sorted.mkString(", ")}")
    })
  }
}
