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
    private val _fileName:String = filename

    private def _fileWrite(str:String) = {
      val fw = new FileWriter(_fileName, true)
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
      )).toString)) {
        print("$level: $event\n")
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
