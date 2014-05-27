package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.{SQSAdapter, SWFAdapter}
import com.amazonaws.services.simpleworkflow.model.ActivityTask
import com.balihoo.fulfillment.config.PropertiesLoader

class NoopWorker(swfAdapter: SWFAdapter, sqsAdapter: SQSAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(task: ActivityTask) = {
    completeTask(task.getTaskToken, "{\"-NOOP-\" : \"true\"}")
  }
}

object noopworker {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".noopworker.properties")
    val noop = new NoopWorker(new SWFAdapter(config), new SQSAdapter(config))
    println("Running noop worker")
    noop.work()
  }
}
