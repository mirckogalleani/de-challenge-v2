package com.walmart.adapters
import org.scalatest.Assertions._
import org.scalatest.FlatSpec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.RuntimeConfig
import com.walmart.exceptions.SessionException


class TestSparkSessionFactory extends FlatSpec {

    "makeSession" should "return SparkSession object when receive local mode" in {
        //Arrange
        val factory = new SparkSessionFactory()
        val mode: String = "local"
        //Act
        val session = factory.makeSession(mode)

        //Assert
        assert(session.isInstanceOf[SparkSession])
        assert(session.conf.get("spark.app.name") == "sparkLocal")
        assert(session.conf.get("spark.sql.shuffle.partitions") == "1")

        session.close()        
    }

    it should "produce an SessionException when receive invalid mode" in {
        //Arrange
        val factory = new SparkSessionFactory()
        val mode: String = "null"

        //Act
        val actual =
        intercept[SessionException] {
            val session = factory.makeSession(mode)

        }
        val expected = "mode must be local or cluster"
        assert(actual.getMessage == expected)
    }

    // test("make local creates spark session as local") {
    //     //Arrange
    //     val factory = new SparkSessionFactory()

    //     //Act
    //     val session = factory.makeLocal()

    //     //Assert
    //     assert(session.isInstanceOf[SparkSession])
    //     assert(session.conf.get("spark.app.name") == "sparkLocal")
    //     assert(session.conf.get("spark.sql.shuffle.partitions") == "1")

    //     session.close()

    // }

    ignore should "return SparkSession object when receive cluster mode" in{ 
        //Arrange
        val factory = new SparkSessionFactory()
        val mode: String = "cluster"
        
        //Act
        val session = factory.makeSession(mode)

        //Assert
        assert(session.isInstanceOf[SparkSession])
        assert(session.conf.get("spark.app.name") == "Pipeline")
        assert(session.conf.get("spark.network.timeout") == "600s")

        session.close()
    }

}