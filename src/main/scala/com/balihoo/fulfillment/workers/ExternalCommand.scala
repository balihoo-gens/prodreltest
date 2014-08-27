package com.balihoo.fulfillment.workers

import java.io.ByteArrayInputStream

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

import scala.sys.process._
/*
 * this is the dependency-injectable class containing all functionality
 */
abstract class AbstractExternalCommand extends FulfillmentWorker {
    this: CommandComponent
    with DynamoAdapterComponent
    with SWFAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    command.getSpecification
  }

  override def handleTask(params: ActivityParameters) = {
    try {
//      val result:String = command.run(params("input"))
      completeTask("83713,90210,77077")
    } catch {
      case exception:Exception =>
        failTask(s"""{"$name": "${exception.toString}"}""", exception.getMessage)
      case t:Throwable =>
        failTask(s"""{"$name": "Caught a Throwable"}""", t.getMessage)
    }
  }
}

class ExternalCommand(swf: SWFAdapter, dyn: DynamoAdapter, commandLine:String)
  extends AbstractExternalCommand
  with SWFAdapterComponent
  with DynamoAdapterComponent
  with CommandComponent {
    lazy val _command = new Command(commandLine)
    def swfAdapter = swf
    def dynamoAdapter = dyn
    def command = _command
}

trait CommandComponent {
  def command: Command

  class Command(commandLine:String) {

    def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new ActivityParameter("input", "string", "Command Input")
      ), new ActivityResult("string", "Result of command execution on 'input'"))
    }

    def run(input:String):String = {
      (commandLine:ProcessBuilder).#<(new ByteArrayInputStream(input.getBytes)).!!
    }
  }
}

object zipcodedemographics {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new ExternalCommand(
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg),
      cfg.getString("commandLine")
    )
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

