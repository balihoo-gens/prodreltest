package com.balihoo.fulfillment.workers.sendgrid

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractSendGridLookupSubaccount extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with SendGridAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new StringParameter("participantId", "The participant ID used to identify the SendGrid subaccount"),
      new BooleanParameter("useTestSubaccount", "True if the SendGrid test account should be used")
    ), new StringResultType("The subaccount username"))
  }

  override def handleTask(args: ActivityArgs):ActivityResult = {
    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    val subaccountId = SendGridSubaccountId(args("participantId"), args[Boolean]("useTestSubaccount"))
    val apiUser = sendGridAdapter.checkSubaccountExists(subaccountId)
    apiUser match {
      case Some(s) => getSpecification.createResult(s)
      case _ => throw new SendGridException("Subaccount not found.") // Throw an exception to cause the task to fail.
    }
  }
}

class SendGridLookupSubaccount(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractSendGridLookupSubaccount
  with LoggingWorkflowAdapterImpl
  with SendGridAdapterComponent {

  private lazy val _sendGridAdapter = new SendGridAdapter(_cfg, _splog)
  def sendGridAdapter = _sendGridAdapter
}

object sendgrid_lookupsubaccount extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new SendGridLookupSubaccount(cfg, splog)
  }
}
