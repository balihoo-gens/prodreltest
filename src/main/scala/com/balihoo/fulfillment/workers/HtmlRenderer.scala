package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger
import scala.io.Source
import java.io._

/*
 * this is the dependency-injectable class containing all functionality
 */
abstract class AbstractHtmlRenderer extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with S3AdapterComponent
  with CommandComponent =>

  def storeScript = {
    val scriptName = swfAdapter.config.getString("scriptName")
    splog("DEBUG", s"using script $scriptName")
    val scriptData = Source.fromURL(getClass.getResource("/" + scriptName))
    val scriptFile = new FileWriter("/tmp/" + scriptName)
    scriptData.getLines.foreach((line:String) => scriptFile.write(s"$line\n"))
    scriptFile.close
    scriptName
  }

  val commandLine = swfAdapter.config.getString("commandLine") + " " + storeScript
  splog.debug(s"Commandline $commandLine")

  override def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new ActivityParameter("source", "string", "The URL of of the page to render"),
        new ActivityParameter("target", "string", "The S3 location of the resulting image")
      ), new ActivityResult("string", "the target URL if successfully saved"))
  }

  override def handleTask(params: ActivityParameters) = {
    try {
      // We're passing the raw JSON string to the command. The command will digest it.
      val cleaninput = params.input.replace("\n","")
      splog.debug(s"running process with ${cleaninput}")
      val result = command.run(cleaninput)
      splog.debug(s"process out: ${result.out}")
      splog.debug(s"process err: ${result.err}")
      result.code match {
        case 0 =>
          //upload it to s3
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
  with S3AdapterComponent
  with CommandComponent {
    lazy val _s3Adapter = new S3Adapter(_cfg)
    def s3Adapter = _s3Adapter
    lazy val _command = new Command(commandLine)
    def command = _command
}

object htmlrenderer extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new HtmlRenderer(cfg, splog)
  }
}
