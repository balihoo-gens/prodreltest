package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

abstract class AbstractNoop extends FulfillmentWorker {
 this: SWFAdapterComponent
   with DynamoAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(), new ActivityResult("JSON", "Confirmation that nothing happened."))
  }

  override def handleTask(task: ActivityParameters) = {
    completeTask("""{"-NOOP-" : "true"}""")
  }
}

class Noop(swf: SWFAdapter, dyn: DynamoAdapter)
  extends AbstractNoop
  with SWFAdapterComponent
  with DynamoAdapterComponent {
    def swfAdapter = swf
    def dynamoAdapter = dyn
}

object noop {
  def main(args: Array[String]) {
    val name = getClass.getSimpleName.stripSuffix("$")
    val cfg = PropertiesLoader(args, name)
    val worker = new Noop(
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg)
    )
    println(s"Running $name")
    worker.work()
  }
}
