package com.walmart

import com.walmart.adapters.{FileFormatOperator, Base64Encoder, SparkSessionFactory}
import com.walmart.exceptions.ControllerException
import com.walmart.models.{Request, Response}
import com.walmart.ports.{Encoder}
import com.walmart.services.ReportService
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read

object App extends Serializable{

    private def makeOperator(session: SparkSession): FileFormatOperator = {
        try{

            val operator: FileFormatOperator = new FileFormatOperator(session)
            return operator
        }
        catch{
            case e:Exception => {
                throw new ControllerException(e.getClass.toString.concat(":").concat(e.getMessage.toString))
            }   
        }
    }

     private def makeRequest(encodedInput: String, encoder: Encoder): Request = {
        try{
            val jsonInput = encoder.decode(encodedInput)
            implicit val formats = Serialization.formats(NoTypeHints)
            return read[Request](jsonInput)
        }
        catch{
            case e:Exception => {
                throw new ControllerException(e.getClass.toString.concat(":").concat(e.getMessage.toString))
            }   
        }
    }


    def main(args: Array[String]): Unit = {

        val encodedInput = args(0)
        try{
            val encoder: Encoder = new Base64Encoder
            val request: Request = this.makeRequest(encodedInput, encoder)
            val sessionFactory = new SparkSessionFactory()
            val session = sessionFactory.makeSession(request.sparkMode)
            val operator: FileFormatOperator = this.makeOperator(session)
            val service: ReportService = new ReportService(operator)
            val response: Response = service.invoke(request)
            if (response.isValid){
                session.stop()
                System.exit(0)
            }else{
                throw new ControllerException(response.toString) 
            }

        }
        catch{
            case e:Exception => {
                throw new ControllerException(e.getMessage)
            }
        }
    }
}
