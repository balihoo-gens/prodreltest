package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger

/*
 * this is the dependency-injectable class containing all functionality
 */
abstract class AbstractZipCodeDemographics extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with CommandComponent =>

  val commandLine = swfAdapter.config.getString("commandLine")

  override def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new ObjectActivityParameter("demographicsQuery", "Demographics Query"),
        new StringActivityParameter("participantId", "Participant Identifier")
      ), new ActivityResult("string", "A list of zip codes 'ZZZZZ,ZZZZZ,ZZZZZ..."))
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
    }
  }
}

class ZipCodeDemographics(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractZipCodeDemographics
  with LoggingWorkflowAdapterImpl
  with CommandComponent {
    lazy val _command = new Command(commandLine)
    def command = _command
}

object zipcodedemographics extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new ZipCodeDemographics(cfg, splog)
  }
}
