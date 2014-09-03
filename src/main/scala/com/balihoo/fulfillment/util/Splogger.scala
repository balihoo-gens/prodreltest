package com.balihoo.fulfillment.util
import com.balihoo.fulfillment.config._
import play.api.libs.json.{Json, JsObject}
import org.joda.time._
import java.io._

class Splogger(filename:String) {
  private val _file:File = _veriFile(filename)

  private def _checkFile(file:File):Boolean = {
    if (file.isFile) {
      //file exists; return the ability to write to it
      file.canWrite
    } else {
      //file doesn't exist, so check the status of the parent dir
      val parentdir = Option(file.getParentFile)
      //if there is no parentdir, use local path
      val path = new File(parentdir.getOrElse(new File(".")).getAbsolutePath)
      if (path.isDirectory) {
        //dir exists; return ability to write
        path.canWrite
      } else {
        //dir does not exist, see if we can make it
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
      val default = s"/var/log/${name}.log"
      println(s"unable to log to $filename; using $default instead")
      new File(default)
    }
  }

  private def _fileWrite(str:String) = {
    var ret = true
    try {
      //FileWriter(..., true) means append
      val fw = new FileWriter(_file, true)
      try fw.write(str)
      //no except needed here;
      // If write throws, fw is closed and ret = false
      finally fw.close()
    }
    catch { case _: Throwable => ret = false }
    ret
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
        print(s"$level: $msg [log parse exception: ${t.getMessage}]\n")
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
