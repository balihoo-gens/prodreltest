package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractSendGridLookupSubaccount extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with SendGridAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new StringActivityParameter("participantId", "The participant ID used to identify the SendGrid subaccount"),
      new BooleanActivityParameter("useTestSubaccount", "True if the SendGrid test account should be used")
    ), new StringActivityResult("The subaccount username"))
  }

  override def handleTask(params: ActivityParameters) = {
    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    withTaskHandling {
      val subaccountId = SendGridSubaccountId(params("participantId"), params[Boolean]("useTestSubaccount"))
      val apiUser = sendGridAdapter.checkSubaccountExists(subaccountId)
      apiUser match {
        case Some(s) => s
        case _ => throw new SendGridException("Subaccount not found.") // Throw an exception to cause the task to fail.
      }
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
