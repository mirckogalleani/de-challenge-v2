package com.walmart.models
import org.scalatest._
import scala.collection.mutable.Map
import com.walmart.exceptions.RequestException

class TestRequest extends FlatSpec with BeforeAndAfter  {

    "constructor" should "return valid object" in {
        //Arrange
        var input: String = "input"
        var output: String = "output"
        var inputFileFormat: String = "inputFileFormat"
        var outputFileFormat: String = "outputFileFormat"
        var sparkMode: String = "local"
        //Act
        val request: Request = new Request(input, output, inputFileFormat, outputFileFormat, sparkMode)

        //Assert
        assert(request.isInstanceOf[Request])
    }

    "addError" should "add an error when receive valid empty or null input" in {
        //Arrange
        val request: Request = new Request()
        val errorMessage = "cannot be null or empty"
        //Act
        request.addError("input", errorMessage)

        //Assert
        assert(request.errorList.size == 1)
        assert(request.errorList("input") == errorMessage)

    }

    it should "add an error when receive valid empty or null output" in {
        //Arrange
        val request: Request = new Request()
        val errorMessage = "cannot be null or empty"
        //Act
        request.addError("output", errorMessage)

        //Assert
        assert(request.errorList.size == 1)
        assert(request.errorList("output") == errorMessage)

    }

    it should "add an error when receive valid empty or null inputFileFormat" in {
        //Arrange
        val request: Request = new Request()
        val errorMessage = "cannot be null or empty"
        //Act
        request.addError("inputFileFormat", errorMessage)

        //Assert
        assert(request.errorList.size == 1)
        assert(request.errorList("inputFileFormat") == errorMessage)

    }

    it should "add an error when receive valid empty or null outputFileFormat" in {
        //Arrange
        val request: Request = new Request()
        val errorMessage = "cannot be null or empty"
        //Act
        request.addError("outputFileFormat", errorMessage)

        //Assert
        assert(request.errorList.size == 1)
        assert(request.errorList("outputFileFormat") == errorMessage)

    }

    it should "add an error when receive valid empty or null sparkMode" in {
        //Arrange
        val request: Request = new Request()
        val errorMessage = "cannot be null or empty"
        //Act
        request.addError("sparkMode", errorMessage)

        //Assert
        assert(request.errorList.size == 1)
        assert(request.errorList("sparkMode") == errorMessage)

    }

    it should "add all errors when receive invalid input to create object" in {
        //Arrange
        var input: String = ""
        var output: String = ""
        var inputFileFormat: String = ""
        var outputFileFormat: String = ""
        var sparkMode: String = ""

        //Act
        val request: Request = new Request(input, output, inputFileFormat, outputFileFormat, sparkMode)
        val errorMessage = "cannot be null or empty"
    


        //Assert
        assert(request.errorList.size == 5)
        assert(request.errorList("input") == errorMessage)
        assert(request.errorList("output") == errorMessage)
        assert(request.errorList("inputFileFormat") == errorMessage)
        assert(request.errorList("outputFileFormat") == errorMessage)
        assert(request.errorList("sparkMode") == errorMessage)

    }



    "isValid" should "return true when errorlist is empty" in {
        //Arrange
        var input: String = "input"
        var output: String = "output"
        var inputFileFormat: String = "inputFileFormat"
        var outputFileFormat: String = "outputFileFormat"
        var sparkMode: String = "local"

        val request: Request = new Request(input, output, inputFileFormat, outputFileFormat, sparkMode)
        val expected: Boolean = true


        //Act
        val actual = request.isValid

        //Assert
        assert(actual == expected)
    }

    it should "produce an RequestException when errorlist is not empty" in {
        //Arrange
        var input: String = "input"
        var output: String = "output"
        var inputFileFormat: String = "inputFileFormat"
        var outputFileFormat: String = ""
        var sparkMode: String = "sparkMode"

        val request: Request = new Request(input, output, inputFileFormat, outputFileFormat, sparkMode)
        //val expected: Boolean = false


        //Act

        val actual =
            intercept[RequestException] {
                val result = request.isValid

            }
        val testMap = Map("outputFileFormat" -> "cannot be null or empty")
        val testClass = "class com.walmart.exceptions.RequestException"
        val expected = "this arguments cannot be empty or null:"
        // assert(actual.getMessage == expected)
        // val actual = request.isValid

        //Assert
        assert(actual.getMessage == expected.concat(testMap.toString))
        //assert(actual.getValue == testClass)
    }

}