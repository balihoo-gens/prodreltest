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
        new ObjectParameter("demographicsQuery", "Demographics Query"),
        new StringParameter("participantId", "Participant Identifier")
      ), new StringsResultType("A array of zip codes"))
  }

  override def handleTask(params: ActivityArgs):ActivityResult = {
    // We're passing the raw JSON string to the command. The command will digest it.
    val result = command.run(params.input)
    result.code match {
      case 0 =>
        getSpecification.createResult(result.out.split(","))
      case 1 => // Special case. We're using 1 to mean CANCEL
        throw new CancelTaskException("Cancelled", result.out)
      case _ =>
        throw new FailTaskException(s"Process returned code '${result.code}'", result.err)
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
