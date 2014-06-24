package com.balihoo.fulfillment.workers
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.{
  SQSAdapter,
  SWFAdapter,
  SESAdapter
}

object test_emailworker {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".emailworker.properties")
    val options = collection.mutable.Map[String, Tuple2[String, () => Unit]] ()

    def usage() = {
      println("Select a test:")
      options.keys.foreach{ k => println(s"[$k] " + options(k)._1)}
    }

    def verifyAddress() = {
      val worker = new VerifyEmailAddressWorker(
        new SWFAdapter(config),
        new SQSAdapter(config),
        new SESAdapter(config)
      )
       println("enter the address to verify")
      val address = readLine("verifyemail> ")
      worker.verifyAddress(address)
      println("check the email address and click the link to complete verification")
    }

    def getValidEmailList() = {
      val worker = new ListVerifiedEmailAddressesWorker(
        new SWFAdapter(config),
        new SQSAdapter(config),
        new SESAdapter(config)
      )
      worker.listVerifiedEmailAddresses()
    }

    def listAddresses() = {
      getValidEmailList.foreach{ s =>
        println(s)
      }
    }

    def sendEmail() = {
      val worker = new SendEmailWorker(
        new SWFAdapter(config),
        new SQSAdapter(config),
        new SESAdapter(config)
      )
      val from = "gens@balihoo.com"
      val subject = "This is an EmailWorker Test"
      val body = "<h1>sup<br>SUP</h1>"
      val validEmails = getValidEmailList()
      var i = 0
      validEmails.foreach{ s =>
        i += 1
        println(s"$i] $s")
      }
      var recipients = List[String]()
      val choices = readLine("addresses> ")
      choices.split(",").foreach{ i =>
        recipients = validEmails(i.toInt - 1) :: recipients
      }
      val res = worker.sendEmail(from, recipients, subject, body)
      println(s"$res\nsent to $recipients")
    }

    options("h") = ("Display Help", usage _)
    options("v") = ("Verify Email Address", verifyAddress _)
    options("s") = ("Send Email", sendEmail _)
    options("l") = ("List Verified Email Addresses", listAddresses _)
    options("q") = ("Quit", () => println("bye"))

    var choice = "h"
    while (choice != "q") {
      if (options.contains(choice)) {
        options(choice)._2()
      } else {
        println("select a valid option. 'h' for help.")
      }
      choice  = readLine("emailtest> ")
    }
  }
}

