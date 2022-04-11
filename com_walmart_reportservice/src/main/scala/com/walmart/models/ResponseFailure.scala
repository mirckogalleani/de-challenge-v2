package com.walmart.models

class ResponseFailure(val value: String, val message: String) extends Response with Serializable {


    def isValid(): Boolean = {
        return false
    }

    def getValue(): String = {
        return this.value
    }

    def getMessage(): String = {
        return this.message
    }

    override def toString(): String ={
        return this.value.concat(":").concat(this.message)
    }
}
