package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.{
  SQSAdapter,
  SWFAdapter,
  SESAdapter,
  RateExceededException
}
import com.balihoo.fulfillment.config.PropertiesLoader

class EmailWorker(swfAdapter: SWFAdapter, sqsAdapter: SQSAdapter, sesAdapter: SESAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(params: ActivityParameters) = {
    println("EmailWorker.handleTask: processing ${name}")

    try {
      name match {
        case "email-send" =>
          val id = sendEmail(
              params.getRequiredParameter("from"),
              params.getRequiredParameter("recipients").split(",").toList,
              params.getRequiredParameter("subject"),
              params.getRequiredParameter("body"),
              params.getRequiredParameter("type") == "html"
          )
          completeTask(s"""{"$name": "${id.toString}"}""")
        case "email-verify-address" =>
          val result:String = verifyAddress(
            params.getRequiredParameter("address")
          )
          completeTask(s"""{"$name": "$result"}""")
        case "email-list-verified-addresses" =>
          val result:String = listVerifiedEmailAddresses().mkString(",")
          completeTask(s"""{"$name": "$result"}""")
        case _ =>
          throw new Exception(s"activity '$name' is NOT IMPLEMENTED")
      }
    } catch {
      case rateExceeded:RateExceededException =>
        cancelTask(s"""{"$name": "RATE EXCEEDED"}""")
      case exception:Exception =>
        failTask(s"""{"$name": "${exception.toString}"}""", exception.getMessage)
      case _:Throwable =>
        failTask(s"""{"$name": "Caught a Throwable""", "caught a throwable")
    }
  }

  def verifyAddress(address: String) = {
    sesAdapter.verifyEmailAddress(address)
  }

  def listVerifiedEmailAddresses(): List[String] = {
    sesAdapter.listVerifiedEmailAddresses()
  }

  def sendEmail(from: String, recipients: List[String], subject: String, body: String, html: Boolean = true): String = {
    sesAdapter.sendEmail(from, recipients, subject, body)
  }
}

object emailworker {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".emailworker.properties")
    val emailworker = new EmailWorker(new SWFAdapter(config), new SQSAdapter(config), new SESAdapter(config))
    println("Running emailworker worker")
    emailworker.work()
  }
}

object test_emailworker {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".emailworker.properties")
    val emailworker = new EmailWorker(new SWFAdapter(config), new SQSAdapter(config), new SESAdapter(config))
    var options = collection.mutable.Map[String, Tuple2[String, () => Unit]] ()

    def usage() = {
      println("Select a test:")
      options.keys.foreach{ k => println(s"[$k] " + options(k)._1)}
    }

    def verifyAddress() = {
      println("enter the address to verify")
      val address = readLine("verifyemail> ")
      emailworker.verifyAddress(address)
    }

    def listAddresses() = {
      emailworker.listVerifiedEmailAddresses().foreach{ s =>
        println(s)
      }
    }

    def sendEmail() = {
      val from = "gens@balihoo.com"
      val subject = "This is an EmailWorker Test"
      val body = "<h1>sup<br>SUP</h1>"
      val validEmails = emailworker.listVerifiedEmailAddresses()
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
      val res = emailworker.sendEmail(from, recipients, subject, body)
      println(s"$res\nsent to $recipients")
    }

    options("h") = ("Display Help", usage)
    options("v") = ("Verify Email Address", verifyAddress)
    options("s") = ("Send Email", sendEmail)
    options("l") = ("List Verified Email Addresses", listAddresses)
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
