package com.balihoo.fulfillment.config

import org.specs2.mutable._

class TestValidSWFIdentifier extends Specification {

  def stringMethod(s: String) = s

  "A name" should {
    "have an implict string conversion" in {
      val name = new ValidSWFName("Hello")
      stringMethod(name) mustEqual("Hello")
    }
    "throw an IllegalArgumentException if it contains illegal characters" in {
      new ValidSWFName("darn") must throwAn[IllegalArgumentException]
    }
    "throw an IllegalArgumentException if it's too long" in {
      new ValidSWFName("1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890" +
        "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890" +
        "123456789012345678901234567890123456789012345678901234567890") must throwAn[IllegalArgumentException]
    }
  }

  "A version" should {
    "have an implict string conversion" in {
      val version = new ValidSWFVersion("64")
      stringMethod(version) mustEqual("64")
    }
    "throw an IllegalArgumentException if it contains illegal characters" in {
      new ValidSWFVersion("qrs##tuv") must throwAn[IllegalArgumentException]
    }
    "throw an IllegalArgumentException if it's too long" in {
      new ValidSWFVersion("1234567890123456789012345678901234567890123456789012345678901234567890") must throwAn[IllegalArgumentException]
    }
  }

}
