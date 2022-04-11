package com.walmart.adapters
import com.walmart.exceptions.FileFormatException
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SparkSession, Dataset, Column, Row, SaveMode}
import org.apache.spark.sql.functions.{col, when, lit, concat, date_format}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DataType}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.commons.lang3.exception.ExceptionUtils

class FileFormatOperator(val session: SparkSession) extends Serializable {

    val sessionErrorMsg = "Session cannot be null"
    val pathNullMsg = "Path cannot be null"
    val pathEmptyMsg = "Path cannot be empty"
    val dataFrameEmptyMsg = "DataFrame cannot be empty"
    val dataFrameNullMsg = "DataFrame cannot be null"
    val columnsEmptyMsg = "Columns cannot be empty"
    val queryNullMsg = "Query cannot be null"
    val queryEmptyMsg = "Query cannot be empty"
    val aliasNullMsg = "Alias cannot be null"
    val aliasEmptyMsg = "Alias cannot be empty"
    val wordsEmptyMsg = "Words cannot be empty"
    val fileFormatNullMsg = "fileFormat cannot be null"
    val fileFormatEmptyMsg = "fileFormat cannot be empty"
    val modeNullMsg = "mode cannot be null"
    val modeEmptyMsg = "mode cannot be empty"
    val partitionedByNullMsg = "partitionedBy cannot be null"
    val partitionedByEmptyMsg = "partitionedBy cannot be empty"
    val headerNullMsg = "header cannot be null"
    val headerEmptyMsg = "header cannot be empty"
    //val fileFormatNotImplementedMsg = s"fileFormat: $fileFormat not Implemented"
    val mylog = Logger("FileFormatOperator")
    
    private def isEmpty(df: DataFrame): Boolean = {
        @transient lazy val validateEmptyDf = df.isEmpty
        return validateEmptyDf
    }

    private def validatePath(path: String): Boolean = {
        path match {
            case null => throw new FileFormatException(pathNullMsg)
            case path if path.isEmpty => throw new FileFormatException(pathEmptyMsg)
            case _ => return true
        }     
    }
    
    private def validateSession(): Boolean = {
        this.session match {
            case null => throw new FileFormatException(sessionErrorMsg)
            case _ => return true
        }     
    }

    private def validateColumns(columns : Seq[String]): Boolean = {
        columns match {
            case columns if columns.isEmpty => throw new FileFormatException(columnsEmptyMsg)
            case _ => return true
        }     
    }

    private def validateFileFormat(fileFormat : String): Boolean = {
        fileFormat match {
            case null => throw new FileFormatException(fileFormatNullMsg)
            case fileFormat if fileFormat.isEmpty => throw new FileFormatException(fileFormatEmptyMsg)
            case fileFormat if fileFormat != "avro" && fileFormat != "csv" && fileFormat != "json" =>  throw new FileFormatException(s"fileFormat: $fileFormat not implemented")
            case _ => return true
        }     
    }


    private def validateMode(mode : String): Boolean = {
        mode match {
            case null => throw new FileFormatException(modeNullMsg)
            case mode if mode.isEmpty => throw new FileFormatException(modeEmptyMsg)
            case _ => return true
        }     
    }


    def getTable(path: String, fileFormat: String, header: Boolean = true): DataFrame = {
        this.validateSession
        this.validatePath(path)
        this.validateFileFormat(fileFormat)
        try{
            var df = this.session.read.format(fileFormat).option("header", header.toString).load(path)
            return df
        }
        catch{
            case x: Exception =>
                {
                    throw new FileFormatException(x.getMessage)
                }
        }
    }

    def saveTable(path: String, df: DataFrame, fileFormat: String, mode: String = "overwrite", header: Boolean = true) = {
        this.isEmpty(df)
        this.validatePath(path)
        this.validateMode(mode)
        this.validateFileFormat(fileFormat)
        try{
            mode match {
                case "overwrite" => {
                    mylog.info(f"saving $path")
                    df.write.mode(SaveMode.Overwrite).format(fileFormat).option("header", header.toString).save(path)
                }
                case "append" => {
                    mylog.info(f"saving $path")
                    df.write.mode(SaveMode.Append).format(fileFormat).option("header", header).save(path)
                }
                case _ => throw new FileFormatException(s"mode: $mode not implemented")
            }

        }
        catch{
            case x: Exception =>
            {
                // mylog.info(x.printStackTrace.toString)
                throw new FileFormatException(x.getMessage)
                
            }
        }
    }



    def existFiles(path: String): Boolean = {
        this.validateSession 
        this.validatePath(path)
        val sc = this.session.sparkContext
        val conf = sc.hadoopConfiguration
        val p: Path  = new Path(path)
        val fs: FileSystem = p.getFileSystem(conf)
        try{
            return fs.exists(p)
        } 
        catch{
            case x: Exception =>
                {
                    throw new FileFormatException("error validating the path")
                }
        }
    }


    def selectColumns(df: DataFrame, columns: Seq[String]): DataFrame = {
        this.isEmpty(df)
        this.validateColumns(columns)
        try{
            
            return df.select(columns.map(x => col(x)).toSeq: _*)
        } 
        catch{
            case x: Exception =>
                {
                    throw new FileFormatException("error selecting the columns")
                }
        }
    }


    def getSchema(table: DataFrame): Seq[String] = {
        this.isEmpty(table)
        return table.schema.map(x => x.name).toSeq

    }

    def close(): Unit = {
        if (this.session == null){
            throw new FileFormatException(sessionErrorMsg)
        }
        else{
            this.session.close()
        }
    }
}