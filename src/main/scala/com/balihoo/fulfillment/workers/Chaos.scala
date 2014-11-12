package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger

import scala.util.Random

abstract class AbstractChaos extends FulfillmentWorker {
  this: LoggingWorkflowAdapter =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new NumberActivityParameter("chanceToFail", "0-100 Chance to Fail"),
      new NumberActivityParameter("chanceToCancel", "0-100 Chance to Cancel")
    ), new ActivityResult("string", "Message of little consequence."))
  }

  override def handleTask(params: ActivityParameters) = {
    val rand = new Random()

    val failActual = rand.nextFloat() * 100
    val cancelActual = rand.nextFloat() * 100

    val failChances = params[Float]("chanceToFail")
    val cancelChances = params[Float]("chanceToCancel")

    if(failActual < failChances) {
      failTask("Chaos ensued!", s"Failing because $failActual < $failChances")
    } else if(cancelActual < cancelChances) {
      cancelTask(s"I don't feel like processing this right now $cancelActual < $cancelChances")
    } else {
      completeTask("Everything went better than expected!")
    }
  }
}

class Chaos(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractChaos
  with LoggingWorkflowAdapterImpl {
}

object chaos extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new Chaos(cfg, splog)
  }
}

