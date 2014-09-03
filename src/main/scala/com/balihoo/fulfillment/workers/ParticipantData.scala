package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

/*
 * this is the dependency-injectable class containing all functionality
 */
abstract class AbstractParticipantData extends FulfillmentWorker {
    this: CommandComponent
    with DynamoAdapterComponent
    with SWFAdapterComponent =>

  val commandLine = swfAdapter.config.getString("commandLine")

  override def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new ActivityParameter("participantId", "int", "Participant Identifier")
      ), new ActivityResult("JSON", "An object of affiliate data"))
  }

  override def handleTask(params: ActivityParameters) = {
    try {
      // We're passing the raw JSON string to the command. The command will digest it.
      val result = command.run(params.input)
      result.code match {
        case 0 =>
          completeTask(result.out)
        case 1 => // Special case. We're using 1 to mean CANCEL
          cancelTask(result.out)
        case _ =>
          failTask(s"Process returned code '${result.code}'", result.err)
      }
    } catch {
      case exception:Exception =>
        failTask(exception.toString, exception.getMessage)
      case t:Throwable =>
        failTask(s"$name Caught a Throwable", t.getMessage)
    }
  }
}

class ParticipantData(swf: SWFAdapter, dyn: DynamoAdapter)
  extends AbstractParticipantData
  with SWFAdapterComponent
  with DynamoAdapterComponent
  with CommandComponent {
    lazy val _command = new Command(commandLine)
    def swfAdapter = swf
    def dynamoAdapter = dyn
    def command = _command
}

object participantdata {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new ParticipantData(
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg)
    )
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

