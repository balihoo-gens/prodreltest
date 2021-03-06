package com.balihoo.fulfillment.workers

import java.net.URI

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger
import play.api.libs.json.{Json, JsObject}
import scala.io.Source
import java.io._
import scala.collection.mutable.{Map => MutableMap}

/*
 * this is the dependency-injectable class containing all functionality
 */
abstract class AbstractHtmlRenderer extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with S3AdapterComponent
  with CommandComponent =>

  val s3bucket = swfAdapter.config.getString("s3bucket")
  val maxsizeDefault = swfAdapter.config.getInt("img_max_size")
  val minqualityDefault = swfAdapter.config.getInt("img_min_quality")

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
    scriptData.getLines().foreach((line:String) => scriptFile.write(s"$line\n"))
    scriptFile.close()
    scriptPath
  }

  def s3Move(imageFileName: String, target: String) = {
    val key:String = "render/" + target
    val file = new File(imageFileName)
    val s3Url = s"https://s3.amazonaws.com/$s3bucket/$key"
    if (file.canRead) {
      splog.info(s"storing $imageFileName into $s3Url")
      s3Adapter.upload(key, file, visibility = PublicS3Visibility).get
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
        new UriParameter("source", "The URL of of the page to render"),
        new StringParameter("clipselector", "The selector used to clip the page", required=false),
        new StringParameter("data", "Optional URLEncoded POST data. Not providing this will use GET", required=false),
        new ObjectParameter("headers", "Optional headers", required=false),
        new IntegerParameter("maxsize", "Maximum size for the image (bytes)", required=false),
        new IntegerParameter("minquality", "Minimum quality of the image (percent)", required=false),
        new StringParameter("target", "The S3 filename of the resulting image")
      ), new StringResultType("the target URL if successfully saved"))
  }

  override def handleTask(args: ActivityArgs):ActivityResult = {
    val target = args[String]("target")
    val source = args[URI]("source")

    val input = MutableMap(
      "action" -> "render",
      "source" -> source.toString,
      "target" -> target
    )

    //optionally add the optional parameters
    for (s <- Seq("data", "clipselector")
         if args.has(s)
    ) yield {
      input(s) = args[String](s)
    }

    if (args.has("headers")) {
      input("headers") = Json.stringify(args[JsObject]("headers"))
    }

    val maxsize = args.getOrElse("maxsize", maxsizeDefault )
    val minquality = args.getOrElse("minquality", minqualityDefault)
    if (maxsize < 0) throw new Exception("max size must be > 0")
    if (minquality < 0) throw new Exception("min quality must be > 0")

    var quality = 100
    var filesize: Long = Long.MaxValue
    var imageFileName: Option[String] = None
    while (filesize > maxsize) {
      if (quality < minquality)
        throw new Exception("Unable to render image within size and quality constraints")
      input("quality") = quality.toString
      val filename = render(input)
      filesize = new File(filename).length
      imageFileName = Some(filename)
      quality = if (quality > minquality) {
        math.max(quality-10,minquality)
      } else {
        minquality - 1
      }
    }

    imageFileName match {
      case Some(filename) =>
        val s3location = s3Move(filename, target)
        getSpecification.createResult(s3location)
      case None =>
        throw new FailTaskException("Failed to render image", s"target=$target")
    }
  }

  def render(input:MutableMap[String,String]): String = {
    val jsinput = Json.stringify(Json.toJson(input.toMap))
    splog.debug(s"running process with $jsinput")
    val result = command.run(jsinput)
    splog.debug(s"process out: ${result.out}")
    splog.debug(s"process err: ${result.err}")
    result.code match {
      case 0 =>
        val jres = Json.parse(result.out)
        (jres \ "result").as[String]
      case _ =>
        throw new Exception(s"Process returned ${result.code}: ${result.err}")
    }
 }
}

class HtmlRenderer(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractHtmlRenderer
  with LoggingWorkflowAdapterImpl
  with S3AdapterComponent
  with CommandComponent {
    lazy val _s3Adapter = new S3Adapter(_cfg, _splog)
    def s3Adapter = _s3Adapter
    lazy val _command = new Command(commandLine)
    def command = _command
}

object htmlrenderer extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new HtmlRenderer(cfg, splog)
  }
}
