package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.{SQSAdapter, SWFAdapter, SESAdapter}
import com.amazonaws.services.simpleworkflow.model.ActivityTask
import com.balihoo.fulfillment.config.PropertiesLoader

class EmailWorker(swfAdapter: SWFAdapter, sqsAdapter: SQSAdapter. emailAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(task: ActivityTask) = {
    completeTask(task.getTaskToken, "{\"-NOOP-\" : \"true\"}")
  }
}

object emailworker {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".emailworker.properties")
    val emailworker = new EmailWorker(new SWFAdapter(config), new SQSAdapter(config), new EmailAdapter(config))
    println("Running emailworker worker")
    emailworker.work()
  }
}
