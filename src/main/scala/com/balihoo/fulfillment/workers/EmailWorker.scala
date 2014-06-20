package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.{SQSAdapter, SWFAdapter, SESAdapter}
import com.amazonaws.services.simpleworkflow.model.ActivityTask
import com.balihoo.fulfillment.config.PropertiesLoader

class EmailWorker(swfAdapter: SWFAdapter, sqsAdapter: SQSAdapter, sesAdapter: SESAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(task: ActivityTask) = {
    completeTask(task.getTaskToken, "{\"-EMAIL-\" : \"true\"}")
  }

  def verifyAddress(address: String) = {
    sesAdapter.verifyEmailAddress(address)
  }

  def listVerifiedEmailAddresses(printToScreen: Boolean): Array[String] = {
    Array()
  }

  def sendEmail(recipients: Array[String], body: String): Boolean = {
    true
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
      emailworker.listVerifiedEmailAddresses(true)
    }

    options("h") = ("Display Help", usage)
    options("v") = ("Verify Email Address", verifyAddress)
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
