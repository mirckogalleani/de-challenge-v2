package com.walmart.models

import org.scalatest.Assertions._
import org.scalatest._

class TestResponseSuccess extends FlatSpec  {

    "constructor" should "return valid object" in {
        //Arrange
        val value = "SUCCESS:"
        val message = "Process runned successfully"       

        //Act
        var actual = new ResponseSuccess(value, message)

        //Assert        
        assert(actual.isInstanceOf[Response])
        assert(actual.value == value)
        assert(actual.message == message)
    }

    "toString" should "return formatted value with message" in {
        //Arrange
        val value = "SUCCESS:"
        val message = "Process runned successfully"          

        //Act
        var actual = new ResponseSuccess(value, message)

        //Assert        
        assert(actual.toString == value.concat(":").concat(message))   
    }

    "isValid" should "return false when responsesuccess is created" in {
        //Arrange
        val value = "SUCCESS:"
        val message = "Process runned successfully"
        //Act
        var actual = new ResponseSuccess(value, message)

        //Assert
        assert(actual.isValid == true)
    }

    "getValue" should "return value when responsesuccess is created" in {
       //Arrange
        val value = "SUCCESS:"
        val message = "Process runned successfully"
        //Act
        var actual = new ResponseSuccess(value, message)

        //Assert
        assert(actual.getValue == value)    
    }

    "getMessage" should "return value when responsesuccess is created" in {
       //Arrange
        val value = "SUCCESS:"
        val message = "Process runned successfully"
        //Act
        var actual = new ResponseSuccess(value, message)

        //Assert
        assert(actual.getMessage == message)    
    }

}