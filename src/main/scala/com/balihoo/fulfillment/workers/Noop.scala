package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractNoop extends FulfillmentWorker {
  this: LoggingWorkflowAdapter =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(), new ObjectResultType("Confirmation that nothing happened."))
  }

  override def handleTask(task: ActivityArgs) = {
    getSpecification.createResult(Map("-NOOP-" -> "true"))
  }
}


class Noop(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractNoop
  with LoggingWorkflowAdapterImpl {
}

object noop extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new Noop(cfg, splog)
  }
}
