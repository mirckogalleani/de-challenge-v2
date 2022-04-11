package com.walmart.exceptions

case class ControllerException(message: String) extends Exception(message)
