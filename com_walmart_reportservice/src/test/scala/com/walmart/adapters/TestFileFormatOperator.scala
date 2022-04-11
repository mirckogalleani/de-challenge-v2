package com.walmart.adapters
import com.walmart.exceptions.FileFormatException
import com.walmart.ports._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf
import org.scalatest._
import scala.collection.mutable.ListBuffer
import org.apache.log4j.{Level, Logger}
import com.typesafe.scalalogging.{Logger => scalaLogger}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DataType, _}

class TestFileFormatOperator extends FlatSpec with BeforeAndAfter {

    val mylog = scalaLogger("TestFileFormatOperator")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    private def makeLocal(): SparkSession = {
        val sparkConf = new SparkConf()
        val appName = "sparkTest"
        sparkConf.set("spark.app.name", appName)
        sparkConf.set("spark.sql.shuffle.partitions", "1")   
        val spark = SparkSession.builder().master("local").config(sparkConf).getOrCreate()

        return spark
    }    
    private def getTable(path: String, spark: SparkSession): DataFrame ={
        if (spark == null){
            throw new FileFormatException("Session cannot be null")
        }
        else{
            if(path == null){
                throw new FileFormatException("Path cannot be null")
            }
            else{
                if(path.isEmpty){
                    throw new FileFormatException("Path cannot be empty")
                }
                else{
                    try{
                        var df = spark.read.format("avro").load(path)
                        return df
                    }
                    catch{
                        case x: Exception =>
                            {
                                throw new FileFormatException("error loading the file")
                            }
                    }
                }
            }
        }
    }

    private def validatePath(path: String, spark: SparkSession): Boolean ={
        if (spark == null){
            throw new FileFormatException("Session cannot be null")
        }
        else{
            if(path == null){
                throw new FileFormatException("Path cannot be null")
            }
            else{
                if(path.isEmpty){
                    throw new FileFormatException("Path cannot be empty")
                }
                else{
                    val sc = spark.sparkContext
                    val conf = sc.hadoopConfiguration
                    val p: Path  = new Path(path)
                    val fs: FileSystem = p.getFileSystem(conf)
                    try{
                        return fs.exists(p)
                    } 
                    catch{
                        case x: Exception =>
                            {
                                throw new FileFormatException("error loading the file")
                            }
                    }
                }
            }
        }
    }

    private def countRowsTable(df: DataFrame): String ={
        return df.count().toString
    }

    private def getSchema(table: DataFrame): Seq[String] = {
        return table.schema.map(x => x.name).toSeq
    }



    "existFiles" should "return true when receive valid path" in {
        
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val path=getClass.getResource("/userdata1.avro").getPath()
        //Act
        val result: Boolean = operator.existFiles(path)
        
        //Assert
        assert(result == true)
    }

    it should "produce an FileFormatException when receive invalid SparkSession" in {
        //Arrange
        val sparkObj: SparkSession = null
        val operator: FileFormatOperator = new FileFormatOperator(sparkObj)
        val path = getClass.getResource("/userdata1.avro").getPath()
        val actual =
        intercept[FileFormatException] {
            operator.existFiles(path)
        }
        val expected = "Session cannot be null"
        assert(actual.getMessage == expected)
    }

    it should "produce an FileFormatException when receive empty path" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val path = ""
        val actual =
        intercept[FileFormatException] {
            operator.existFiles(path)
        }
        val expected = "Path cannot be empty"
        assert(actual.getMessage == expected)
    }

    it should "produce an FileFormatException when receive null path" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val path = null
        val actual =
        intercept[FileFormatException] {
            operator.existFiles(path)
        }
        val expected = "Path cannot be null"
        assert(actual.getMessage == expected)
    }

    "getTable" should "return an Spark DataFrame when receive valid path" in {
        
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val path = getClass.getResource("/userdata1.avro").getPath()
        val fileFormat = "avro"
        //Act
        val result: DataFrame = operator.getTable(path, fileFormat)

        //Assert
        assert(result.getClass.getSimpleName == "Dataset")
    }

    it should "produce an FileFormatException when receive invalid SparkSession" in {
        //Arrange
        val path = getClass.getResource("/userdata1.avro").getPath()
        val session: SparkSession = null
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val fileFormat = "avro"
        val actual =
        intercept[FileFormatException] {
            operator.getTable(path, fileFormat)
        }
        val expected = "Session cannot be null"
        assert(actual.getMessage == expected)
    }

    it should "produce an FileFormatException when receive empty path" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val path = ""
        val fileFormat = "avro"
        val actual =
        intercept[FileFormatException] {
            operator.getTable(path, fileFormat)
        }
        val expected = "Path cannot be empty"
        assert(actual.getMessage == expected)
    }

    it should "produce an FileFormatException when receive null path" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val path = null
        val fileFormat = "avro"
        val actual =
        intercept[FileFormatException] {
            operator.getTable(path, fileFormat)
        }
        val expected = "Path cannot be null"
        assert(actual.getMessage == expected)
    }

    it should "produce an FileFormatException when receive empty fileFormat" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val path = getClass.getResource("/userdata1.avro").getPath()
        val fileFormat = ""
        val actual =
        intercept[FileFormatException] {
            operator.getTable(path, fileFormat)
        }
        val expected = "fileFormat cannot be empty"
        assert(actual.getMessage == expected)
    }

    it should "produce an FileFormatException when receive null fileFormat" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val path = getClass.getResource("/userdata1.avro").getPath()
        val fileFormat = null
        val actual =
        intercept[FileFormatException] {
            operator.getTable(path, fileFormat)
        }
        val expected = "fileFormat cannot be null"
        assert(actual.getMessage == expected)
    }

    it should "produce an FileFormatException when receive invalid fileFormat" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val path = getClass.getResource("/userdata1.avro").getPath()
        val fileFormat = "notImplemented"
        val actual =
        intercept[FileFormatException] {
            operator.getTable(path, fileFormat)
        }
        val expected = s"fileFormat: $fileFormat not implemented"
        assert(actual.getMessage == expected)
    }


    "saveTable" should "return true when receive valid path and DataFrame and mode and fileFormat" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val path = getClass.getResource("/").getPath().concat("output_table/")
        val fileFormat = "avro"
        import session.implicits._
        val actual = Seq(
            (8, "bat"),
            (64, "mouse"),
            (-27, "horse")
            ).toDF("number", "word")

        //Act
        operator.saveTable(path, actual, fileFormat)
        val validateTableResult: Boolean = this.validatePath(path, session)
        val expected: DataFrame = this.getTable(path, session)

        //Assert
        assert(validateTableResult == true || this.countRowsTable(actual) == this.countRowsTable(expected)) 
    }


    it should "produce an FileFormatException when receive empty path" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val fileFormat = "json"
        import session.implicits._
        val df = Seq(
            (8, "bat"),
            (64, "mouse"),
            (-27, "horse")
            ).toDF("number", "word")
        val path=""
        val actual =
        intercept[FileFormatException] {
            operator.saveTable(path, df, fileFormat)
        }
        val expected = "Path cannot be empty"
        assert(actual.getMessage == expected)
    }

    it should "produce an FileFormatException when receive null path" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val fileFormat = "json"
        import session.implicits._
        val df = Seq(
            (8, "bat"),
            (64, "mouse"),
            (-27, "horse")
            ).toDF("number", "word")
        val path= null
        val actual =
        intercept[FileFormatException] {
            operator.saveTable(path, df, fileFormat)
        }
        val expected = "Path cannot be null"
        assert(actual.getMessage == expected)
    }

    it should "produce an FileFormatException when receive null mode" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val mode = null
        val path = getClass.getResource("/").getPath().concat("output_table/")
        val fileFormat = "json"
        import session.implicits._
        val df = Seq(
            (8, "bat"),
            (64, "mouse"),
            (-27, "horse")
            ).toDF("number", "word")
        val actual =
        intercept[FileFormatException] {
            operator.saveTable(path, df, fileFormat, mode)
        }
        val expected = "mode cannot be null"
        assert(actual.getMessage == expected)
    }

    it should "produce an FileFormatException when receive empty mode" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val mode = ""
        val path = getClass.getResource("/").getPath().concat("output_table/")
        val fileFormat = "json"
        import session.implicits._
        val df = Seq(
            (8, "bat"),
            (64, "mouse"),
            (-27, "horse")
            ).toDF("number", "word")
        val actual =
        intercept[FileFormatException] {
            operator.saveTable(path, df, fileFormat, mode)
        }
        val expected = "mode cannot be empty"
        assert(actual.getMessage == expected)
    }


    it should "produce an FileFormatException when receive invalid mode" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val mode = "notImplemented"
        val path = getClass.getResource("/").getPath().concat("output_table/")
        val fileFormat = "json"
        import session.implicits._
        val df = Seq(
            (8, "bat"),
            (64, "mouse"),
            (-27, "horse")
            ).toDF("number", "word")
        val actual =
        intercept[FileFormatException] {
            operator.saveTable(path, df, fileFormat, mode)
        }
        val expected = s"mode: $mode not implemented"
        assert(actual.getMessage == expected)
    }

    it should "produce an FileFormatException when receive null fileFormat" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val mode = "overwrite"
        val path = getClass.getResource("/").getPath().concat("output_table/")
        val fileFormat = null
        import session.implicits._
        val df = Seq(
            (8, "bat"),
            (64, "mouse"),
            (-27, "horse")
            ).toDF("number", "word")
        val actual =
        intercept[FileFormatException] {
            operator.saveTable(path, df, fileFormat, mode)
        }
        val expected = "fileFormat cannot be null"
        assert(actual.getMessage == expected)
    }

    it should "produce an FileFormatException when receive empty fileFormat" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val mode = "overwrite"
        val path = getClass.getResource("/").getPath().concat("output_table/")
        val fileFormat = ""
        import session.implicits._
        val df = Seq(
            (8, "bat"),
            (64, "mouse"),
            (-27, "horse")
            ).toDF("number", "word")
        val actual =
        intercept[FileFormatException] {
            operator.saveTable(path, df, fileFormat, mode)
        }
        val expected = "fileFormat cannot be empty"
        assert(actual.getMessage == expected)
    }


    it should "produce an FileFormatException when receive invalid fileFormat" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val mode = "overwrite"
        val path = getClass.getResource("/").getPath().concat("output_table/")
        val fileFormat = "invalid"
        import session.implicits._
        val df = Seq(
            (8, "bat"),
            (64, "mouse"),
            (-27, "horse")
            ).toDF("number", "word")
        val actual =
        intercept[FileFormatException] {
            operator.saveTable(path, df, fileFormat, mode)
        }
        val expected = s"fileFormat: $fileFormat not implemented"
        assert(actual.getMessage == expected)
    }

    "selectColumns" should "return a dataframe when receive valid dataframe and valid columns" in {
        
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val path=getClass.getResource("/").getPath().concat("/userdata1.avro")
        val actualDF: DataFrame = this.getTable(path, session)
        val columns: Map[String, DataType] = Map[String, DataType](
            "registration_dttm" -> StringType,
            "id" -> StringType,
            "first_name" -> StringType
        )
        
        //Act
        val columnsNames = columns.keys.toSeq
        val expectedDF: DataFrame = operator.selectColumns(actualDF, columnsNames)
        val expectedColumns = ListBuffer[String]()
        for(column<- columnsNames){
            var expectedName: String = expectedDF.schema.filter(x => x.name == column)(0).name
            expectedColumns += expectedName

        }


        //Assert
        assert(columnsNames.equals(expectedColumns.toList.toSeq))
    }

    it should "produce an FileFormatException when receive empty columns" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val path=getClass.getResource("/").getPath().concat("/userdata1.avro")
        val actualDF: DataFrame = this.getTable(path, session)
        
        val columns: Map[String, DataType] = Map[String, DataType]()
        val columnsNames = columns.keys.toSeq
        //Act
        val actual =
        intercept[FileFormatException] {
            operator.selectColumns(actualDF, columnsNames)
        }
        val expected = "Columns cannot be empty"
        //Assert
        assert(actual.getMessage == expected)
    }


     it should "produce an FileFormatException when receive bad columns names" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        val path = getClass.getResource("/").getPath().concat("/userdata1.avro")
        val actualDF: DataFrame = this.getTable(path, session)
        val columns: Map[String, DataType] = Map[String, DataType](
            "wrongcolumna" -> StringType,
            "wrongcolumnb" -> StringType,
            "wrongcolumnc" -> StringType
        )
        val columnsNames = columns.keys.toSeq
        //Act
        val actual =
        intercept[FileFormatException] {
            operator.selectColumns(actualDF, columnsNames)
            }
        val expected = "error selecting the columns"
        //Assert
        assert(actual.getMessage == expected)
    }



    "getSchema" should "return the schema for a valid dataframe" in {
        //Arrange
        val session = this.makeLocal()
        val operator: FileFormatOperator = new FileFormatOperator(session)
        import session.implicits._
        val df = Seq(
            (8, "bat"),
            (64, "mouse"),
            (-27, "horse")
            ).toDF("number", "word")
        val expected = Seq(
            "number",
            "word"
        )

        //Act
        val actual = operator.getSchema(df)

        //Assert
        assert(actual.equals(expected))
    }
    
}


    


