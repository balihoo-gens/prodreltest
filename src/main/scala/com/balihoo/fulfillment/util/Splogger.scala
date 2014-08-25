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
    //FileWriter(..., true) means append
    val fw = new FileWriter(_file, true)
    try {
      fw.write(str)
      true
    }
    catch {
      case _: Throwable => false
    }
    finally fw.close()
  }

  def apply(level: String, msg: String) = {
    log(level, msg)
  }

  def log(level: String, msg: String) = {
    try {
      val now:DateTime = new DateTime(DateTimeZone.UTC)
      //{"utctime": "2014-08-20 23:42:50.030868", "level": "INFO", "event": "doing stuff" }
      val jstr = Json.stringify(Json.toJson(Map(
        "utctime" -> now.toString,
        "level" -> level,
        "event" -> msg
      )))

      if (!_fileWrite(s"$jstr\n")) {
        println(jstr)
      }
    }
    catch {
      case t: Throwable =>
        print(s"$level: $msg [$t.getMessage]\n")
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
