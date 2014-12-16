package com.balihoo.fulfillment.config

import org.junit.runner.RunWith
import org.keyczar.Crypter
import org.keyczar.util.Base64Coder
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TestEncryption extends Specification {

  "Encryption" should {
    "successfully encrypt/decrypt" in {

      val input = "The tuna flesh glistens in the moonlight"
      val crypter = new Crypter("config/crypto")
      val ciphertext = crypter.encrypt(input)
      val plain = crypter.decrypt(ciphertext)

      input mustNotEqual ciphertext
      input mustEqual plain
    }

    "generate compatible keys" in {

      // If we ever want/need new encryption credentials this will do the trick
      val aes_bytes = new Array[Byte](16)
      Random.nextBytes(aes_bytes)
      val aes_string = Base64Coder.encode(aes_bytes)
      println(s"AES(128 bit): $aes_string\n")

      val hmac_bytes = new Array[Byte](32)
      Random.nextBytes(hmac_bytes)
      val hmac_string = Base64Coder.encode(hmac_bytes)
      println(s"HMAC(256 bit): $hmac_string\n")

      aes_bytes mustNotEqual new Array[Byte](16)
      hmac_bytes mustNotEqual new Array[Byte](32)

      aes_string mustNotEqual aes_bytes
      hmac_string mustNotEqual hmac_bytes
    }

    "t" in {

      val appSecret = "09340934fopijfpioj34f0t934poijwe5gpoijrfpokefa4wp9k34fvpoke4f"
      val accessToken = "98349834lkidflkj3q4f0934094flke0p9w4f4opij34f9j34f0934fmervlgkjergf5opji34op9j345io34fjio3498043to0we4rtsdrf"
      val crypter = new Crypter("config/crypto")
      println("\n\n"+crypter.encrypt(appSecret)+"\n\n")
      println(crypter.encrypt(accessToken)+"\n\n")

      success
    }
  }

}
