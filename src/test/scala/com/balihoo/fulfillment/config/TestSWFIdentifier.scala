package com.balihoo.fulfillment.config

import org.specs2.mutable._

class TestSWFIdentifier extends Specification {

  def stringMethod(s: String) = s

  "An SWFName" should {
    "have an implict string conversion" in {
      val name = new SWFName("Hello")
      stringMethod(name) mustEqual("Hello")
    }
    "throw an IllegalArgumentException if it contains illegal characters" in {
      new SWFName("darn") must throwAn[IllegalArgumentException]
    }
    "throw an IllegalArgumentException if it's too long" in {
      new SWFName("1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890" +
        "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890" +
        "123456789012345678901234567890123456789012345678901234567890") must throwAn[IllegalArgumentException]
    }
  }

  "An SWFVersion" should {
    "have an implict string conversion" in {
      val version = new SWFVersion("64")
      stringMethod(version) mustEqual("64")
    }
    "throw an IllegalArgumentException if it contains illegal characters" in {
      new SWFVersion("qrs##tuv") must throwAn[IllegalArgumentException]
    }
    "throw an IllegalArgumentException if it's too long" in {
      new SWFVersion("1234567890123456789012345678901234567890123456789012345678901234567890") must throwAn[IllegalArgumentException]
    }
  }

}
