package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger
import play.api.libs.json.{Json, JsObject}
import scala.io.Source
import java.io._

/*
 * this is the dependency-injectable class containing all functionality
 */
abstract class AbstractHtmlRenderer extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with S3AdapterComponent
  with CommandComponent =>

  val s3bucket = swfAdapter.config.getString("s3bucket")

  /**
    * gets the script name from the config file, finds in in the resources
    * and saves is to /tmp/ so it can be executed cmd line with phantomjs
    */
  def storeScript = {
    val scriptName = swfAdapter.config.getString("scriptName")
    val scriptPath = s"$scriptName"
    splog.debug(s"using script $scriptName")
    val scriptData = Source.fromURL(getClass.getResource("/" + scriptName))
    val scriptFile = new FileWriter(scriptPath)
    scriptData.getLines.foreach((line:String) => scriptFile.write(s"$line\n"))
    scriptFile.close
    scriptPath
  }

  def s3Move(imageFileName: String, target: String) = {
    val key:String = "render/" + target
    val file = new File(imageFileName)
    val s3Url = s"https://s3.amazonaws.com/$s3bucket/$key"
    if (file.canRead) {
      splog.info(s"storing $imageFileName into $s3Url")
      s3Adapter.putPublic(s3bucket, key, file)
      file.delete
    } else {
      throw new Exception(s"Unable to store rendered image to S3: $imageFileName does not exist")
    }
    s3Url
  }

  val commandLine = swfAdapter.config.getString("commandLine") + " " + storeScript
  splog.debug(s"Commandline $commandLine")

  override def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new ActivityParameter("source", "string", "The URL of of the page to render"),
        new ActivityParameter("clipselector", "string", "The selector used to clip the image", false),
        new ActivityParameter("target", "string", "The S3 filename of the resulting image")
      ), new ActivityResult("string", "the target URL if successfully saved"))
  }

  override def handleTask(params: ActivityParameters) = {
    try {
      val cliptuple = if (params.has("clipselector")) { (
        "clipselector", params("clipselector"))
      } else {
        ("ignore", "undefined")
      }

      val cleaninput = Json.stringify(Json.toJson(Map(
        "action" -> "render",
        "source" -> params("source"),
        "target" -> params("target"),
        cliptuple
      )))

      splog.debug(s"running process with ${cleaninput}")
      val result = command.run(cleaninput)
      splog.debug(s"process out: ${result.out}")
      splog.debug(s"process err: ${result.err}")
      result.code match {
        case 0 =>
          val jres = Json.parse(result.out)
          val imageFileName = (jres \ "result").as[String]
          val s3location = s3Move(imageFileName, params("target"))
          completeTask(s3location)
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
