package com.walmart.services
import com.walmart.adapters.{SparkSessionFactory, FileFormatOperator}
import com.walmart.exceptions._
import com.walmart.models._
import org.apache.spark.sql.types.StringType
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import scala.collection.JavaConverters._
import org.scalamock.scalatest._

class TestReportService extends FlatSpec with MockFactory{
    val mylog = Logger("TestReportService")

    private def makeScoreReport(table: DataFrame): DataFrame = {
        //count the score points of the teams separated by home and away
        val hdfpoints = table.withColumn("POINTS", when(col("FTR")==="H", 3).when(col("FTR")==="A", 0)when(col("FTR")==="D", 1)).withColumn("SHOTRATIO", col("FTHG") / col("HST"))
        val adfpoints = table.withColumn("POINTS", when(col("FTR")==="H", 0).when(col("FTR")==="A", 3)when(col("FTR")==="D", 1)).withColumn("SHOTRATIO", col("FTAG") / col("AST"))

        //total points grouped by team and season 
        val home_points = hdfpoints.select("HOMETEAM","AWAYTEAM", "HS", "AS", "HST", "AST", "FTR", "FTHG", "FTAG", "POINTS", "DATE", "SEASON", "SHOTRATIO").groupBy(col("HOMETEAM").as("TEAM"), col("SEASON")).agg(sum(col("POINTS")) as "TOTALPOINTS", sum(col("HST")) as "SHOTSTARGET", avg(col("SHOTRATIO")) as "AVGSHOTRATIO", sum(col("FTAG")) as "SHOTSRECEIVED", sum(col("FTHG")) as "SHOTSMADE")
        val away_points = adfpoints.select("HOMETEAM","AWAYTEAM", "HS", "AS", "HST", "AST", "FTR", "FTHG", "FTAG", "POINTS", "DATE", "SEASON", "SHOTRATIO").groupBy(col("AWAYTEAM").as("TEAM"), col("SEASON")).agg(sum(col("POINTS")) as "TOTALPOINTS", sum(col("AST")) as "SHOTSTARGET", avg(col("SHOTRATIO")) as "AVGSHOTRATIO", sum(col("FTHG")) as "SHOTSRECEIVED", sum(col("FTAG")) as "SHOTSMADE")
        
        //result data with points, shots target, avg shot ratio, shots received, shots made and shot ratio grouped by team and season
        val result = home_points.union(away_points).groupBy("TEAM","SEASON").agg(sum(col("TOTALPOINTS")).as("POINTS"), sum(col("SHOTSTARGET")) as "SHOTSTARGET", avg(col("AVGSHOTRATIO")) as "AVGSHOTRATIO", sum(col("SHOTSRECEIVED")) as "SHOTSRECEIVED", sum(col("SHOTSMADE")) as "SHOTSMADE" ).orderBy(desc("SEASON")).withColumn("SHOTRATIO", col("SHOTSMADE") / col("SHOTSTARGET"))

        return result
    }

    private def makeShotRatioReport(table: DataFrame): DataFrame = {

        val shot_ratio_by_season = table.groupBy("SEASON").agg(max(col("SHOTRATIO")) as "SHOTRATIO").orderBy(desc("SHOTRATIO"))
        val shotRatioReport = shot_ratio_by_season.join(table, shot_ratio_by_season("SEASON") === table("SEASON") && shot_ratio_by_season("SHOTRATIO") === table("SHOTRATIO")).select(table("TEAM"),shot_ratio_by_season("SEASON"),shot_ratio_by_season("SHOTRATIO")).orderBy(desc("SHOTRATIO"))
        return shotRatioReport
    }

    private def makeMostReceivedShotsReport(table: DataFrame): DataFrame = {

        val shot_received_by_season = table.groupBy("SEASON").agg(max(col("SHOTSRECEIVED")) as "SHOTSRECEIVED").orderBy(desc("SHOTSRECEIVED"))
        val shotsReceivedReport = shot_received_by_season.join(table, shot_received_by_season("SEASON") === table("SEASON") && shot_received_by_season("SHOTSRECEIVED") === table("SHOTSRECEIVED")).select(table("TEAM"),shot_received_by_season("SEASON"),shot_received_by_season("SHOTSRECEIVED")).orderBy(desc("SHOTSRECEIVED"))
        return shotsReceivedReport
    }


    private def makeMostMadeShotsReport(table: DataFrame): DataFrame = {

        val made_shots_by_season = table.groupBy("SEASON").agg(max(col("SHOTSMADE")) as "SHOTSMADE").orderBy(desc("SHOTSMADE"))
        val madeShotsReport = made_shots_by_season.join(table, made_shots_by_season("SEASON") === table("SEASON") && made_shots_by_season("SHOTSMADE") === table("SHOTSMADE")).select(table("TEAM"),made_shots_by_season("SEASON"),made_shots_by_season("SHOTSMADE")).orderBy(desc("SHOTSMADE"))
        return madeShotsReport
    }

    "constructor" should "return valid object" in {
        //Arrange

        //Act
        val operator: FileFormatOperator = mock[FileFormatOperator]
        val request: Request = mock[Request]

        val service = new ReportService(operator)
        //Assert
        assert(service.isInstanceOf[ReportService])
    }

    it should "raises an IllegalArgumentException when receives null operator" in {
        //Arrange
        val operator: FileFormatOperator = null
        val request: Request = mock[Request]

        //Assert
        assertThrows[IllegalArgumentException] {
            val service = new ReportService(operator)
        }
    }





    "invoke" should "return response success on valid adapters and request" in {
        //Arrange
        var input: String = "input"
        var output: String = "output"
        var inputFileFormat: String = "json"
        var outputFileFormat: String = "json"
        var sparkMode: String = "local"
        var header: Boolean = true
        var mode: String = "overwrite"
        val columns_points: List[String] = List("TEAM", "SEASON", "POINTS")
        val request: Request = new Request(input, output, inputFileFormat, outputFileFormat, sparkMode)

        val mockRequest: Request = mock[Request]

        val factory = new SparkSessionFactory()
        val session = factory.makeSession(sparkMode)
        // val df = session.emptyDataFrame
        // val data = Array(0, 8, 0, 3, 2, 0, "Sunderland", 8.5, 4.5, 1.4, 8.5, 4.33, 1.4, 9.0, 4.6, 1.35, 39, "23", "-1.25", 2.05, 1.76, 8.37, "1.91", "1.96", 4.47, 1.4, 2.15, 1.83, 9.5, "1.96", "2.02", 4.89, 1.44, 33, "18/08/12", "E0", 0, 0, "D", "9", "4.6", "1.35", 7, 12, 0, 14, 4, 0, 0, "D", 0, "Arsenal", 7.3, 4.5, 1.35, "8", "4.5", "1.4", 8.78, 8.71, 4.72, 1.44, 4.76, 1.44, "C Foy", null, null, null, "9.5", "4.5", "1.36", 8.5, 4.75, 1.44, 8.0, 4.0, 1.44, 1213)
        // val rdd_data = session.sparkContext.parallelize(data)
        val schema = StructType(Array(StructField("AC", LongType, true),
            StructField("AF", LongType, true),
            StructField("AR", LongType, true),
            StructField("AS", LongType, true),
            StructField("AST", LongType, true),
            StructField("AY", LongType, true),
            StructField("AwayTeam", StringType, true),
            StructField("B365A", DoubleType, true),
            StructField("B365D", DoubleType, true),
            StructField("B365H", DoubleType, true),
            StructField("BSA", DoubleType, true),
            StructField("BSD", DoubleType, true),
            StructField("BSH", DoubleType, true),
            StructField("BWA", DoubleType, true),
            StructField("BWD", DoubleType, true),
            StructField("BWH", DoubleType, true),
            StructField("Bb1X2", LongType, true),
            StructField("BbAH", StringType, true),
            StructField("BbAHh", StringType, true),
            StructField("BbAv<2.5", DoubleType, true),
            StructField("BbAv>2.5", DoubleType, true),
            StructField("BbAvA", DoubleType, true),
            StructField("BbAvAHA", StringType, true),
            StructField("BbAvAHH", StringType, true),
            StructField("BbAvD", DoubleType, true),
            StructField("BbAvH", DoubleType, true),
            StructField("BbMx<2.5", DoubleType, true),
            StructField("BbMx>2.5", DoubleType, true),
            StructField("BbMxA", DoubleType, true),
            StructField("BbMxAHA", StringType, true),
            StructField("BbMxAHH", StringType, true),
            StructField("BbMxD", DoubleType, true),
            StructField("BbMxH", DoubleType, true),
            StructField("BbOU", LongType, true),
            StructField("Date", StringType, true),
            StructField("Div", StringType, true),
            StructField("FTAG", LongType, true),
            StructField("FTHG", LongType, true),
            StructField("FTR", StringType, true),
            StructField("GBA", StringType, true),
            StructField("GBD", StringType, true),
            StructField("GBH", StringType, true),
            StructField("HC", LongType, true),
            StructField("HF", LongType, true),
            StructField("HR", LongType, true),
            StructField("HS", LongType, true),
            StructField("HST", LongType, true),
            StructField("HTAG", LongType, true),
            StructField("HTHG", LongType, true),
            StructField("HTR", StringType, true),
            StructField("HY", LongType, true),
            StructField("HomeTeam", StringType, true),
            StructField("IWA", DoubleType, true),
            StructField("IWD", DoubleType, true),
            StructField("IWH", DoubleType, true),
            StructField("LBA", StringType, true),
            StructField("LBD", StringType, true),
            StructField("LBH", StringType, true),
            StructField("PSA", DoubleType, true),
            StructField("PSCA", DoubleType, true),
            StructField("PSCD", DoubleType, true),
            StructField("PSCH", DoubleType, true),
            StructField("PSD", DoubleType, true),
            StructField("PSH", DoubleType, true),
            StructField("Referee", StringType, true),
            StructField("SBA", DoubleType, true),
            StructField("SBD", DoubleType, true),
            StructField("SBH", DoubleType, true),
            StructField("SJA", StringType, true),
            StructField("SJD", StringType, true),
            StructField("SJH", StringType, true),
            StructField("VCA", DoubleType, true),
            StructField("VCD", DoubleType, true),
            StructField("VCH", DoubleType, true),
            StructField("WHA", DoubleType, true),
            StructField("WHD", DoubleType, true),
            StructField("WHH", DoubleType, true)))
        val df = session.createDataFrame(session.sparkContext.emptyRDD[Row], schema)
        val operator = mock[FileFormatOperator]
        request.isValid
        (operator.getTable _) expects(request.input, request.inputFileFormat, header) returning(df)

        val data_with_seasons = df.withColumn("SEASON", regexp_extract(input_file_name, "season-(\\d+)_json.json$", 1))
        val result = this.makeScoreReport(data_with_seasons)

        //val total_report = result.select("TEAM", "SEASON", "POINTS")
        (operator.selectColumns _) expects(*, columns_points.toSeq) returning(df)

        val shot_ratio_report = this.makeShotRatioReport(result)

        val shots_received_report = this.makeMostReceivedShotsReport(result)

        val made_shots_report = this.makeMostMadeShotsReport(result)

        (operator.saveTable _) expects(request.output.concat("total_report/"), *, outputFileFormat, mode, header)
        (operator.saveTable _) expects(request.output.concat("shot_ratio_report/"), *, outputFileFormat, mode, header)
        (operator.saveTable _) expects(request.output.concat("shots_received_report/"), *, outputFileFormat, mode, header)
        (operator.saveTable _) expects(request.output.concat("made_shots_report/"), *, outputFileFormat, mode, header)

        (operator.close _) expects()
        val service = new ReportService(operator)

        //Act
        val response = service.invoke(request)

        //Assert
        assert(response.isInstanceOf[ResponseSuccess])
        assert(response.isValid() == true)
        assert(response.getValue() == "ReportService")
        assert(response.getMessage() == "runned successfully")
    }

    it should "produce an responseFailure when receive invalid request" in {

        //Arrange
        var input: String = ""
        var output: String = "output"
        var inputFileFormat: String = "inputFileFormat"
        var outputFileFormat: String = "outputFileFormat"
        var sparkMode: String = "local"
        var header: Boolean = true
        var mode: String = "mode"

        val request: Request = new Request(input, output, inputFileFormat, outputFileFormat, sparkMode)

        val mockRequest: Request = mock[Request]

        val factory = new SparkSessionFactory()
        val session = factory.makeSession(sparkMode)
        val df = session.emptyDataFrame
        val operator = mock[FileFormatOperator]
        val service = new ReportService(operator)

        //Act
        val response = service.invoke(request)

        //Assert
        assert("class com.walmart.exceptions.RequestException" == response.getValue)
    }

    it should "produce an responseFailure when operator exception" in {
        
        //Arrange
        var input: String = "input"
        var output: String = "output"
        var inputFileFormat: String = "inputFileFormat"
        var outputFileFormat: String = "outputFileFormat"
        var sparkMode: String = "local"
        var header: Boolean = true
        var mode: String = "mode"

        val request: Request = new Request(input, output, inputFileFormat, outputFileFormat, sparkMode)

        val mockRequest: Request = mock[Request]

        val factory = new SparkSessionFactory()
        val session = factory.makeSession(sparkMode)
        val df = session.emptyDataFrame
        val operator = mock[FileFormatOperator]
        (operator.getTable _) expects(input, inputFileFormat, header) throwing(new FileFormatException("Path cannot be empty"))
        val service = new ReportService(operator)

        //Act
        val response = service.invoke(request)

        //Assert
        assert("class com.walmart.exceptions.FileFormatException" == response.getValue)
    }


}
