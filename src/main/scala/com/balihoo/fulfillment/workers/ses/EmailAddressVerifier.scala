package com.balihoo.fulfillment.workers.ses

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractEmailAddressVerifier extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with SESAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new EmailParameter("address", "Address to be verified with SES")
    ), new ObjectResultType("Result of Verification"))
  }

  override def handleTask(args: ActivityArgs):ActivityResult = {
    splog.debug(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    getSpecification.createResult(verifyAddress(args[String]("address")))
  }

  def verifyAddress(address: String):String = {
    sesAdapter.verifyEmailAddress(address)
  }
}

class EmailAddressVerifier(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractEmailAddressVerifier
  with LoggingWorkflowAdapterImpl
  with SESAdapterComponent {
    private lazy val _ses = new SESAdapter(_cfg)
    def sesAdapter = _ses
}

object email_addressverifier extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new EmailAddressVerifier(cfg, splog)
  }
}

