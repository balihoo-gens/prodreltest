package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

abstract class AbstractNoopWorker extends FulfillmentWorker {
 this: SWFAdapterComponent
   with DynamoAdapterComponent =>

  override def handleTask(task: ActivityParameters) = {
    completeTask("""{"-NOOP-" : "true"}""")
  }
}

class NoopWorker(swf: SWFAdapter, dyn: DynamoAdapter)
  extends AbstractNoopWorker
  with SWFAdapterComponent
  with DynamoAdapterComponent {
    def swfAdapter = swf
    def dynamoAdapter = dyn
}

object noopworker {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new NoopWorker(
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg)
    )
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}
