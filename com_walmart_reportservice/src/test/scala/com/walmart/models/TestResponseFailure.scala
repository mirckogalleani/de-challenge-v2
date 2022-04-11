package com.walmart.models

import org.scalatest.Assertions._
import org.scalatest._

class TestResponseFailure extends FlatSpec  {
    "constructor" should "return valid object" in {
        //Arrange
        val value = "com.walmart.exceptions.EncoderException"
        val message = "Input cannot be empty"       

        //Act
        var actual = new ResponseFailure(value, message)

        //Assert        
        assert(actual.isInstanceOf[Response])
        assert(actual.value == value)
        assert(actual.message == message)
    }

    "toString" should "return formatted value with message" in {
        //Arrange
        val value = "com.walmart.exceptions.EncoderException"
        val message = "Input cannot be empty"       

        //Act
        var actual = new ResponseFailure(value, message)

        //Assert        
        assert(actual.toString == value.concat(":").concat(message))   
    }

    "isValid" should "return false when responsefailure is created" in {
        //Arrange
        val value = "com.walmart.exceptions.EncoderException"
        val message = "Input cannot be empty"
        //Act
        var actual = new ResponseFailure(value, message)

        //Assert
        assert(actual.isValid == false)
    }
    
    "getValue" should "return value when responsefailure is created" in {
       //Arrange
        val value = "com.walmart.exceptions.EncoderException"
        val message = "Input cannot be empty"
        //Act
        var actual = new ResponseFailure(value, message)

        //Assert
        assert(actual.getValue == value)    
    }

    "getMessage" should "return value when responsefailure is created" in {
       //Arrange
        val value = "com.walmart.exceptions.EncoderException"
        val message = "Input cannot be empty"
        //Act
        var actual = new ResponseFailure(value, message)

        //Assert
        assert(actual.getMessage == message)    
    }


}