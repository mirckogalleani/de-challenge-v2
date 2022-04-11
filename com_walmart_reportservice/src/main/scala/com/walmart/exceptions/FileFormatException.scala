package com.walmart.exceptions

case class FileFormatException(message: String) extends Exception(message)