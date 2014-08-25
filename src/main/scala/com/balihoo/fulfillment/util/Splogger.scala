package com.balihoo.fulfillment.util
import com.balihoo.fulfillment.config._
import play.api.libs.json.{Json, JsObject}
import org.joda.time._
import java.io._

/* TODO cake pattern
trait SploggerComponent {
  def splog: AbstractSplogger with PropertiesLoaderComponent

  abstract class AbstractSplogger(filename:String ) {
    this: PropertiesLoaderComponent =>
*/
  class Splogger(filename:String ) {
    private val _file:File = _checkFile(filename)

    private def _checkFile(filename:String):File = {
      var file = new File(filename)
      val path = new File(file.getAbsolutePath)
      if (!path.isDirectory) {
          if (!path.mkdirs) {
            val name = """[\W]""".r.replaceAllIn(file.getName, "")
            val default = s"/tmp/splogger_default_${name}.log"
            println(s"unable to log to $filename; using $default instead")
            file = new File(default)
          }
      }
      file
    }

    private def _fileWrite(str:String) = {
     val fw = new FileWriter(_file, true)
      var retval = true
      try fw.write(str)
      catch {
        case _:Throwable => { retval = false }
      }
      finally fw.close()
      retval
    }

    def log(level: String, msg: String) = {
      val now:DateTime = new DateTime(DateTimeZone.UTC)
      //{"utctime": "2014-08-20 23:42:50.030868", "level": "INFO", "event": "doing stuff" }
      if (!_fileWrite(Json.toJson(Map(
        "utctime" -> now.toString,
        "level" -> level,
        "event" -> msg
      )).toString + "\n")) {
        print(s"$level: $msg\n")
      }
     }

    def debug(msg:String) = {
      log("DEBUG", msg)
    }

    def info(msg:String) = {
      log("INFO", msg)
    }

    def warn(msg:String) = {
      log("WARN", msg)
    }

    def error(msg:String) = {
      log("ERROR", msg)
    }

    def exception(msg:String) = {
      log("EXCEPTION", msg)
    }

  }
// }
