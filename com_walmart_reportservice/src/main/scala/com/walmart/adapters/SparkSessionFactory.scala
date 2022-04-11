package com.walmart.adapters
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.walmart.exceptions.SessionException

class SparkSessionFactory {
    val invalidModeMsg: String = "mode must be local or cluster"

    def makeSession(mode: String): SparkSession = {
        if (mode == "local"){
            return this.makeLocal();
        } else if (mode == "cluster"){
            this.makeCluster();
        } else {
            throw new SessionException(invalidModeMsg)
        }
    }

    private def makeCluster(): SparkSession = {
        val sparkConf = new SparkConf()
        val appName = "Pipeline"
        sparkConf.set("spark.app.name", appName)
        sparkConf.set("spark.debug.maxToStringField", "200" )   
        sparkConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        sparkConf.set("spark.network.timeout", "600s")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        return spark
    }

    private def makeLocal(): SparkSession = {
        val sparkConf = new SparkConf()
        val appName = "sparkLocal"
        sparkConf.set("spark.app.name", appName)
        sparkConf.set("spark.sql.shuffle.partitions", "1")   
        val spark = SparkSession.builder().master("local").config(sparkConf).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        return spark
    }

}