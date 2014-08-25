package com.balihoo.fulfillment.util
import com.balihoo.fulfillment.config._
import play.api.libs.json.{Json, JsObject}
import org.joda.time._
import java.io._

class Splogger(filename:String) {
  private val _file:File = _veriFile(filename)

  private def _checkFile(file:File):Boolean = {
    if (file.isFile) {
      file.canWrite
    } else {
      val path = new File(file.getAbsolutePath)
      if (path.isDirectory) {
        path.canWrite
      } else {
        path.mkdirs
      }
    }
  }

  private def _veriFile(filename:String):File = {
    val file = new File(filename)
    if (_checkFile(file)) {
      file
    } else {
      val name = """[\W]""".r.replaceAllIn(filename, "_")
      val default = s"/tmp/${name}.log"
      println(s"unable to log to $filename; using $default instead")
      new File(default)
    }
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

  def apply(level: String, msg: String) = {
    log(level, msg)
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
