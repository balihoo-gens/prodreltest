package com.balihoo.fulfillment.util
import com.balihoo.fulfillment.config._
import play.api.libs.json.{Json, JsObject}
import org.joda.time._
import java.io._

/**
 * Cake component providing a splunk logger
 */
trait SploggerComponent {
  def splog: Splogger
}

/**
 * Splunk compatible logger
 */
class Splogger(filename:String) {

  /**
   * the file that is logged to
   */
  private val _file:File = _veriFile(filename)

  /**
   * @return ability to access or create the file
   */
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

  /**
   * @return A valid file, either with the filename provided or
   *   a default filename created in the current directory based
   *   on the filename / path provided
   */
  private def _veriFile(filename:String):File = {
    val file = new File(filename)
    if (_checkFile(file)) {
      file
    } else {
      val name = """[\W]""".r.replaceAllIn(filename, "_")
      val default = s"${name}.log"
      println(s"unable to log to $filename; using $default instead")
      new File(default)
    }
  }

  /**
   * safely attempts to write a string to
   * _file, ensuring it is closed after
   * @return success
   */
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

  /**
   * allow calling this class directly
   */
  def apply(level: String, msg: String) = {
    log(level, msg)
  }


  /**
   * format a splunk compatible json string
   * and write it to a file. if writing fails
   * write to stdout. write formatting exceptions to stdout
   * but do not print the original string in case that was the source of
   * the exception
   */
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


  /**
   * shorthand for debug messages
   */
  def debug(msg:String) = {
    log("DEBUG", msg)
  }

  /**
   * shorthand for info messages
   */
  def info(msg:String) = {
    log("INFO", msg)
  }

  /**
   * shorthand for warning messages
   */
  def warn(msg:String) = {
    log("WARN", msg)
  }

  /**
   * shorthand for error messages
   */
  def error(msg:String) = {
    log("ERROR", msg)
  }

  /**
   * shorthand for exception messages
   */
  def exception(msg:String) = {
    log("EXCEPTION", msg)
  }
}

/**
 * companion object for static methods
 */
object Splogger {

  /**
   * use regex to clean up a name appropriate for a log file
   * and form it into a full-path log file name in the canonical
   * fulfillment location.
   * @param name any identifier on which to base the log file name
   * @returns the full log file path
   */
  def mkFFName(name:String): String = {
    val cleanName = """[\W]""".r.replaceAllIn(name, "_")
    s"/var/log/balihoo/fulfillment/${cleanName}.log"
  }
}
