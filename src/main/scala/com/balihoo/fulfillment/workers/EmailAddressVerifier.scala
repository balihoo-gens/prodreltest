package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractEmailAddressVerifier extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with SESAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new StringActivityParameter("address", "Address to be verified with SES")
    ), new ActivityResult("JSON", "Result of Verification"))
  }

  override def handleTask(params: ActivityParameters) = {
    println(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    withTaskHandling {
      verifyAddress(params("address"))
    }
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

