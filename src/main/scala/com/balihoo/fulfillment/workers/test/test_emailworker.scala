package com.balihoo.fulfillment.workers.test

import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.workers.{
  EmailSender,
  EmailAddressVerifier,
  EmailVerifiedAddressLister
}
import com.amazonaws.services.simpleworkflow.model.{
  StartWorkflowExecutionRequest,
  TaskList,
  WorkflowType
}

import scala.io.Source

object email {
  def main(args: Array[String]) {
    println("Running SendEmailWorker")
    val cfg = PropertiesLoader(args, "email")
    val options = collection.mutable.Map[String, Tuple2[String, () => Unit]] ()

    def usage() = {
      println("Select a test:")
      options.keys.foreach{ k => println(s"[$k] " + options(k)._1)}
    }

    def verifyAddress() = {
      val worker = new EmailAddressVerifier
        with SWFAdapterComponent with DynamoAdapterComponent with SESAdapterComponent {
          def swfAdapter = new SWFAdapter with PropertiesLoaderComponent { def config = cfg }
          def dynamoAdapter = new DynamoAdapter with PropertiesLoaderComponent { def config = cfg }
          def sesAdapter = new SESAdapter with PropertiesLoaderComponent { def config = cfg }
        }
      println("enter the address to verify")
      val address = readLine("verifyemail> ")
      worker.verifyAddress(address)
      println("check the email address and click the link to complete verification")
    }

    def getValidEmailList() = {
      val worker = new EmailVerifiedAddressLister
        with SWFAdapterComponent with DynamoAdapterComponent with SESAdapterComponent {
          def swfAdapter = new SWFAdapter with PropertiesLoaderComponent { def config = cfg }
          def dynamoAdapter = new DynamoAdapter with PropertiesLoaderComponent { def config = cfg }
          def sesAdapter = new SESAdapter with PropertiesLoaderComponent { def config = cfg }
        }
      worker.listVerifiedEmailAddresses()
    }

    def listAddresses() = {
      getValidEmailList.foreach{ s =>
        println(s)
      }
    }

    def sendEmail() = {
      val worker = new EmailSender
        with SWFAdapterComponent with DynamoAdapterComponent with SESAdapterComponent {
          def swfAdapter = new SWFAdapter with PropertiesLoaderComponent { def config = cfg }
          def dynamoAdapter = new DynamoAdapter with PropertiesLoaderComponent { def config = cfg }
          def sesAdapter = new SESAdapter with PropertiesLoaderComponent { def config = cfg }
        }
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

    def submitTask() = {
      object swf extends SWFAdapter with PropertiesLoaderComponent { def config = cfg }
      println("enter the json input filename")
      val inputfile = readLine("inputfile> ")
      val input = try {
        Source.fromFile(inputfile.trim).mkString
      } catch {
        case e:Exception => {
          println(e.getMessage)
          ""
        }
      }

      if (input.length() > 0) {
        swf.client.startWorkflowExecution(
          new StartWorkflowExecutionRequest()
            .withDomain(swf.domain)
            .withWorkflowId("test_emailworker")
            .withInput(input)
            .withExecutionStartToCloseTimeout("1600")
            .withTaskList(
              new TaskList()
                .withName("default_tasks")
            )
            .withWorkflowType(
              new WorkflowType()
                .withName("emailtest")
                .withVersion("1")
            )
        )
      }
    }

    options("h") = ("Display Help", usage _)
    options("v") = ("Verify Email Address", verifyAddress _)
    options("s") = ("Send Email", sendEmail _)
    options("l") = ("List Verified Email Addresses", listAddresses _)
    options("t") = ("submit a json email task to the decider", submitTask _)
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

