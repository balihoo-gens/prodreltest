package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger

/*
 * this is the dependency-injectable class containing all functionality
 */
abstract class AbstractHtmlRenderer extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with CommandComponent =>

  val commandLine = swfAdapter.config.getString("commandLine")

  override def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new ActivityParameter("URL", "string", "The URL of of the page to render")
      ), new ActivityResult("string", "the URL of the rendered image"))
  }

  override def handleTask(params: ActivityParameters) = {
    try {
      // We're passing the raw JSON string to the command. The command will digest it.
      val result = command.run(params.input)
      result.code match {
        case 0 =>
          completeTask(result.out)
        case 1 => // Special case. We're using 1 to mean CANCEL
          cancelTask(result.out)
        case _ =>
          failTask(s"Process returned code '${result.code}'", result.err)
      }
    } catch {
      case exception:Exception =>
        failTask(exception.toString, exception.getMessage)
    }
  }
}

class HtmlRenderer(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractHtmlRenderer
  with LoggingWorkflowAdapterImpl
  with CommandComponent {
    lazy val _command = new Command(commandLine)
    def command = _command
}

object htmlrenderer extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new HtmlRenderer(cfg, splog)
  }
}
