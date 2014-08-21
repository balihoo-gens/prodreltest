package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import scala.util.Random

abstract class AbstractChaos extends FulfillmentWorker {
 this: SWFAdapterComponent
   with DynamoAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ActivityParameter("chanceToFail", "float", "0-100 Chance to Fail"),
      new ActivityParameter("chanceToCancel", "float", "0-100 Chance to Cancel")
    ), new ActivityResult("string", "Message of little consequence."))
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

class Chaos(swf: SWFAdapter, dyn: DynamoAdapter)
  extends AbstractChaos
  with SWFAdapterComponent
  with DynamoAdapterComponent {
    def swfAdapter = swf
    def dynamoAdapter = dyn
}

object chaos {
  def main(args: Array[String]) {
    val name = getClass.getSimpleName.stripSuffix("$")
    val cfg = PropertiesLoader(args, name)
    val worker = new Chaos(
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg)
    )
    println(s"Running $name")
    worker.work()
  }
}
