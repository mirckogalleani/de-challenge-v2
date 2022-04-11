package com.walmart.adapters
import com.walmart.models._
import com.walmart.ports._
import com.walmart.exceptions._
import org.scalatest.Assertions._
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfter
import org.scalamock.scalatest.MockFactory

class TestBase64Encoder extends FlatSpec with BeforeAndAfter {

    "constructor" should "return valid object" in {
        //Arrange

        //Act
        val encoder: Base64Encoder = new Base64Encoder()

        //Assert
        assert(encoder.isInstanceOf[Base64Encoder])
    }

    "encode" should "return encoded in base 64 string if receive valid input" in {
        
        //Arrange
        val encoder: Base64Encoder = new Base64Encoder()
        val input: String = """{"path":"/Development/cl/walmart/dummydata/dummydatabase/","date":"2020/05/19"}"""
        val expected: String = "eyJwYXRoIjoiL0RldmVsb3BtZW50L2NsL3dhbG1hcnQvZHVtbXlkYXRhL2R1bW15ZGF0YWJhc2UvIiwiZGF0ZSI6IjIwMjAvMDUvMTkifQ=="
        
        //Act
        val actual: String = encoder.encode(input)
        
        //Assert
        assert(actual == expected)
    }
    
    it should "return EncoderException when receive empty input" in {
        
        //Arrange
        val encoder: Base64Encoder = new Base64Encoder()
        val input: String = ""
  
        //Act
        val actual =
            intercept[EncoderException] {
                val result: String = encoder.encode(input)
            }
        val expected = "Input cannot be empty"

        //Assert
        assert(actual.getMessage == expected)
    }

    it should "return EncoderException when receive null input" in {
        
        //Arrange
        val encoder: Base64Encoder = new Base64Encoder()
        val input: String = null
  
        //Act
        val actual =
            intercept[EncoderException] {
                val result: String = encoder.encode(input)
            }
        val expected = "Input cannot be null"

        //Assert
        assert(actual.getMessage == expected)
    }

    "decode" should "return decoded in base 64 string if receive valid input" in {
        
        //Arrange
        val encoder: Base64Encoder = new Base64Encoder()
        val input: String = "eyJwYXRoIjoiL0RldmVsb3BtZW50L2NsL3dhbG1hcnQvZHVtbXlkYXRhL2R1bW15ZGF0YWJhc2UvIiwiZGF0ZSI6IjIwMjAvMDUvMTkifQ=="
        val expected: String = """{"path":"/Development/cl/walmart/dummydata/dummydatabase/","date":"2020/05/19"}"""
        
        //Act
        val actual: String = encoder.decode(input)
        
        //Assert
        assert(actual == expected)
    }
    
    it should "return EncoderException when receive empty input" in {
        
        //Arrange
        val encoder: Base64Encoder = new Base64Encoder()
        val input: String = ""
  
        //Act
        val actual =
            intercept[EncoderException] {
                val result: String = encoder.decode(input)
            }
        val expected = "Input cannot be empty"

        //Assert
        assert(actual.getMessage == expected)
    }

    it should "return EncoderException when receive null input" in {
        
        //Arrange
        val encoder: Base64Encoder = new Base64Encoder()
        val input: String = null
  
        //Act
        val actual =
            intercept[EncoderException] {
                val result: String = encoder.decode(input)
            }
        val expected = "Input cannot be null"

        //Assert
        assert(actual.getMessage == expected)
    }
}