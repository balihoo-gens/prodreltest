package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.{SQSAdapter, SWFAdapter}
import com.balihoo.fulfillment.config.PropertiesLoader
import scala.util.Random

class ChaosWorker(swfAdapter: SWFAdapter, sqsAdapter: SQSAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

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

object chaosworker {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".chaosworker.properties")
    val chaos = new ChaosWorker(new SWFAdapter(config), new SQSAdapter(config))
    println("Running chaos worker")
    chaos.work()
  }
}
