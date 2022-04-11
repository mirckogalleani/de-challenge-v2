package com.walmart.services
import com.walmart.adapters.FileFormatOperator
import com.walmart.models.{Request, Response, ResponseFailure, ResponseSuccess}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame

class ReportService() extends Serializable {
    var fileOperator: FileFormatOperator = _
    val mylog = Logger("ReportService")
    def this(fileOperator: FileFormatOperator){
        this()
        if (fileOperator == null) {
            throw new IllegalArgumentException("file operator cannot be null")
        }
        this.fileOperator = fileOperator
        
    }

    
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

    def invoke(request: Request): Response = {

        try {
            request.isValid
            val data = fileOperator.getTable(request.input, request.inputFileFormat)
            val data_with_seasons = data.withColumn("SEASON", regexp_extract(input_file_name, "season-(\\d+)_json.json$", 1))
            
            val result = this.makeScoreReport(data_with_seasons)
            
            val columns_points: List[String] = List("TEAM", "SEASON", "POINTS")
            
            val total_report = fileOperator.selectColumns(result, columns_points.toSeq)

            val shot_ratio_report = this.makeShotRatioReport(result)

            val shots_received_report = this.makeMostReceivedShotsReport(result)

            val made_shots_report = this.makeMostMadeShotsReport(result)

            fileOperator.saveTable(request.output.concat("total_report/"), total_report, request.outputFileFormat, "overwrite")
            
            fileOperator.saveTable(request.output.concat("shot_ratio_report/"), shot_ratio_report, request.outputFileFormat, "overwrite")

            fileOperator.saveTable(request.output.concat("shots_received_report/"), shots_received_report, request.outputFileFormat, "overwrite")

            fileOperator.saveTable(request.output.concat("made_shots_report/"), made_shots_report, request.outputFileFormat, "overwrite")
            mylog.info("|Score Report by Season|")
            total_report.show(1000, false)
            mylog.info("|Shot Ratio Report by Season|")
            shot_ratio_report.show(1000, false)
            mylog.info("|Shots Received Report by Season|")
            shots_received_report.show(1000, false)
            mylog.info("|Made Shots Report by Season|")
            made_shots_report.show(1000, false)

            fileOperator.close()

            val response = new ResponseSuccess("ReportService","runned successfully")
            return response
        }
        catch {
                case e:Exception => {
                    val response = new ResponseFailure(e.getClass.toString, e.getMessage.toString)

                return response
            }
        }
    }
}
