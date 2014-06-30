package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.{DynamoAdapter, SWFAdapter}
import com.balihoo.fulfillment.config.PropertiesLoader
import scala.util.Random

class ChaosWorker(swfAdapter: SWFAdapter, dynamoAdapter: DynamoAdapter)
  extends FulfillmentWorker(swfAdapter, dynamoAdapter) {

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
    val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val chaos = new ChaosWorker(new SWFAdapter(config), new DynamoAdapter(config))
    println("Running chaos worker")
    chaos.work()
  }
}
