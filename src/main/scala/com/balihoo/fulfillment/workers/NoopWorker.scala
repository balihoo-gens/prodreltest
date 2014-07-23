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
        lazy val swfAdapter = new SWFAdapter with PropertiesLoaderComponent { lazy val config = cfg }
        lazy val dynamoAdapter = new DynamoAdapter with PropertiesLoaderComponent { lazy val config = cfg }
      }
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}
