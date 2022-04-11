package com.walmart.ports

trait Encoder {
    def encode(input: String): String
    def decode(input: String): String
}
