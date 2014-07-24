package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

abstract class NoopWorker extends FulfillmentWorker with SWFAdapterComponent with DynamoAdapterComponent {

  override def handleTask(task: ActivityParameters) = {
    completeTask("""{"-NOOP-" : "true"}""")
  }
}

object noopworker {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new NoopWorker
      with SWFAdapterComponent with DynamoAdapterComponent {
        def swfAdapter = new SWFAdapter with PropertiesLoaderComponent { def config = cfg }
        def dynamoAdapter = new DynamoAdapter with PropertiesLoaderComponent { def config = cfg }
      }
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}
