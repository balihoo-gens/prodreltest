package com.balihoo.fulfillment.workers.test

import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.workers.HtmlRenderer
import com.amazonaws.services.simpleworkflow.model.{
  StartWorkflowExecutionRequest,
  TaskList,
  WorkflowType
}

import scala.io.Source

object htmltest {
  def renderHtml(cfg: PropertiesLoader, splog: Splogger) = {
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
      val swfAdapter = new SWFAdapter(cfg, splog)
      swfAdapter.client.startWorkflowExecution(
        new StartWorkflowExecutionRequest()
          .withDomain(swfAdapter.domain)
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

  def main(args: Array[String]) {
    val name = getClass.getSimpleName.stripSuffix("$")
    println("Running HtmlRenderer Test")
    val cfg = PropertiesLoader(args, "htmlrenderer")
    val splog = new Splogger(Splogger.mkFFName(name))
    renderHtml(cfg, splog)
  }
}

