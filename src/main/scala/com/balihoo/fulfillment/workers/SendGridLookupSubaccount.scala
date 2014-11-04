package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractSendGridLookupSubaccount extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with SendGridAdapterComponent =>

  val _cfg: PropertiesLoader

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ActivityParameter("participantId", "string", "The participant ID used to identify the SendGrid subaccount"),
      new ActivityParameter("useTestAccount", "boolean", "True if the SendGrid test account should be used")
    ), new ActivityResult("boolean", "True if the SendGrid subaccount exists"))
  }

  override def handleTask(params: ActivityParameters) = {
    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    withTaskHandling {
      val participantId = params("participantId")
      val useTestSubaccount = params("useTestSubaccount").toBoolean
      sendGridAdapter.checkAccountExists(SendGridSubaccount(participantId, useTestSubaccount)).toString
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
    new FacebookPoster(cfg, splog)
  }
}
