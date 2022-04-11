package com.walmart.models
import scala.collection.mutable.Map
import com.walmart.exceptions.RequestException

class Request() extends Serializable {

    var errorList: Map[String, String] = Map.empty[String, String]
    val errorMessage = "cannot be null or empty"

    var input: String = _
    var output: String = _
    var inputFileFormat: String = _
    var outputFileFormat: String = _
    var sparkMode: String = _

    def addError(field: String, message: String) = {
        errorList += (field -> message)
    }

    def this(input: String, output: String, inputFileFormat: String, outputFileFormat: String, sparkMode: String){
        this()

        if (input == null || input.isEmpty) {
            addError("input", errorMessage)
        } else {
            this.input = input
        }

        if (output == null || output.isEmpty) {
            addError("output", errorMessage)
        } else {
            this.output = output
        }

        if (inputFileFormat == null || inputFileFormat.isEmpty) {
            addError("inputFileFormat", errorMessage)
        } else {
            this.inputFileFormat = inputFileFormat
        }

        if (outputFileFormat == null || outputFileFormat.isEmpty) {
            addError("outputFileFormat", errorMessage)
        } else {
            this.outputFileFormat = outputFileFormat
        }

        if (sparkMode == null || sparkMode.isEmpty) {
            addError("sparkMode", errorMessage)
        } else {
            this.sparkMode = sparkMode
        }
    }

    def isValid(): Boolean = {
        if (!this.errorList.isEmpty){
            throw new RequestException("this arguments cannot be empty or null:".concat(this.errorList.toString))
            return false
        }
        else{
            return this.errorList.isEmpty
        }
    }

}