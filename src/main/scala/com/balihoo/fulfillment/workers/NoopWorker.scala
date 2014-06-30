package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.{DynamoAdapter, SWFAdapter}
import com.balihoo.fulfillment.config.PropertiesLoader

class NoopWorker(swfAdapter: SWFAdapter, dynamoAdapter: DynamoAdapter)
  extends FulfillmentWorker(swfAdapter, dynamoAdapter) {

  override def handleTask(task: ActivityParameters) = {
    completeTask("""{"-NOOP-" : "true"}""")
  }
}

object noopworker {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val noop = new NoopWorker(new SWFAdapter(config), new DynamoAdapter(config))
    println("Running noop worker")
    noop.work()
  }
}
