package com.walmart.exceptions

case class RequestException(message: String) extends Exception(message)