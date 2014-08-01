package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import scala.util.Random

abstract class AbstractChaosWorker extends FulfillmentWorker {
 this: SWFAdapterComponent
   with DynamoAdapterComponent =>

  override def handleTask(params: ActivityParameters) = {
    val rand = new Random()

    if(rand.nextBoolean()) {
      completeTask("Complete!!!")
    } else if(rand.nextBoolean()) {
      cancelTask("Cancelling!!!")
    } else {
      failTask("Failing!!", "For reasons..")
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
