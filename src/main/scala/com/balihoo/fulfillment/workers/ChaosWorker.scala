package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.{SQSAdapter, SWFAdapter}
import com.amazonaws.services.simpleworkflow.model.ActivityTask
import com.balihoo.fulfillment.config.PropertiesLoader
import scala.util.Random

class ChaosWorker(swfAdapter: SWFAdapter, sqsAdapter: SQSAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(task: ActivityTask) = {
    val rand = new Random()

    if(rand.nextBoolean()) {
      completeTask(task.getTaskToken, "Complete!!!")
    } else if(rand.nextBoolean()) {
      cancelTask(task.getTaskToken, "Cancelling!!!")
    } else {
      failTask(task.getTaskToken, "Failing!!", "For reasons..")
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
