// package com.walmart.ports
// import org.apache.spark.sql.DataFrame
// import org.apache.spark.sql.types._
// import org.apache.spark.sql.Column

// trait FileFormatOperator {
//     def getTable(path: String): DataFrame
//     def saveTable(path: String, df: DataFrame)
//     def castColumns(df: DataFrame, columns: scala.collection.immutable.Map[Column, DataType]): DataFrame
//     def transformColumns(df: DataFrame, columns: Seq[String], function: String => String): DataFrame
//     def renameColumns(df: DataFrame, columns: Map[String, String]): DataFrame 
//     def copyColumns(df: DataFrame, columns: Map[String, String]): DataFrame
//     def concatWordsToColumn(df: DataFrame, column: String, words: Seq[String]): DataFrame
//     def createColumnWithSingleValue(df: DataFrame, column: String, value: String): DataFrame
//     def close()
// }