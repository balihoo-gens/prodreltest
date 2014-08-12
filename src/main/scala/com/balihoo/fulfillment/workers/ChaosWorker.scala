package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import scala.util.Random

abstract class AbstractChaosWorker extends FulfillmentWorker {
 this: SWFAdapterComponent
   with DynamoAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ActivityParameter("chanceToFail", "float", "0-100 Chance to Fail"),
      new ActivityParameter("chanceToCancel", "float", "0-100 Chance to Cancel")
    ))
  }

  override def handleTask(params: ActivityParameters) = {
    val rand = new Random()

    val failActual = rand.nextFloat() * 100
    val cancelActual = rand.nextFloat() * 100

    val failChances = params("chanceToFail").toFloat
    val cancelChances = params("chanceToCancel").toFloat

    if(failActual < failChances) {
      failTask("Chaos ensued!", s"Failing because $failActual < $failChances")
    } else if(cancelActual < cancelChances) {
      cancelTask(s"I don't feel like processing this right now $cancelActual < $cancelChances")
    } else {
      completeTask("Everything went better than expected!")
    }
  }
}

class ChaosWorker(swf: SWFAdapter, dyn: DynamoAdapter)
  extends AbstractChaosWorker
  with SWFAdapterComponent
  with DynamoAdapterComponent {
    def swfAdapter = swf
    def dynamoAdapter = dyn
}

object chaosworker {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new ChaosWorker(
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg)
    )
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}
