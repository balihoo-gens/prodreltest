package com.balihoo.fulfillment.config

//import java.util.Properties
import org.apache.commons.configuration.PropertiesConfiguration
import java.io.{File, FileInputStream, InputStream}
import scopt._
import scala.util.Try

//for the cake pattern dependency injection
trait PropertiesLoaderComponent {
  def config: PropertiesLoader
}

class PropertiesLoader(propertiesFileName: String, propertiesDir: String = "") {

  private val prop: PropertiesConfiguration = new PropertiesConfiguration()
  private var propertiesFileStream: InputStream = null
  try {
    propertiesFileStream = new FileInputStream(propertiesFileName)
    prop.setBasePath(propertiesDir)
    prop.load(propertiesFileStream)
  } catch {
      case exception:Exception =>
        println(s"${exception.toString}, ${exception.getMessage}")
      case _:Throwable =>
        println(s"caught throwable")
   } finally {
    if (propertiesFileStream != null)
      propertiesFileStream.close()
  }

  def getInt(propName: String) : Int = {
    val propVal: String = prop.getString(propName)
    if (propVal == null)
      throw new Exception(s"required config value '$propName' not found or blank in $propertiesFileName")
    Integer.parseInt(propVal)
  }

  def getString(propName: String) : String = {
    val propVal: String = prop.getString(propName)
    if (propVal == null)
      throw new Exception(s"required config value '$propName' not found or blank in $propertiesFileName")
    propVal
  }

  def getOrElse(propName: String, default: String) : String = {
    val propVal: String = prop.getString(propName)
    Try(propVal.toString).getOrElse(default)
  }

  def getOrElse(propName: String, default: Int) : Int = {
    val propVal: String = prop.getString(propName)
    Try(propVal.toInt).getOrElse(default)
  }

  def getOrElse(propName: String, default: Boolean) : Boolean = {
    val propVal: String = prop.getString(propName)
    Try(propVal.toBoolean).getOrElse(default)
  }

  def getOrElse(propName: String, default: Long) : Long = {
    val propVal: String = prop.getString(propName)
    Try(propVal.toLong).getOrElse(default)
  }

  @deprecated("Use getOrElse", "10/14/2014")
  def getOptInt(propName: String, defaultValue:Int) : Int = {
    val propVal: String = prop.getString(propName)
    if (propVal == null)
      defaultValue
    else
      Integer.parseInt(propVal)
  }

  @deprecated("Use getOrElse", "10/14/2014")
  def getOptString(propName: String, defaultValue: String) : String = {
    val propVal: String = prop.getString(propName)
    if (propVal == null)
      defaultValue
    else
      propVal
  }
}

//companion object here to avoid this boilerplate from being copied into every worker main
object PropertiesLoader {
  def apply(args: Array[String], progname:String):PropertiesLoader = {
    case class Config(
      propfilename: String = progname,
      propdir: String = "config"
    )

    val parser = new scopt.OptionParser[Config](progname) {
      head("test", "1.0")
      opt[String]('p', "propfile") action { (x, c) =>
        c.copy(propfilename = x) } text("propfile is your properties file")
      opt[String]('d', "propdir") action { (x, c) =>
        c.copy(propfilename = x) } text("propdir is your properties directory")
    }

    parser.parse(args, Config()) map { config =>
      val filename = s"${config.propdir}/${config.propfilename}.properties"
      new PropertiesLoader(filename, config.propdir)
      //TODO: add support for multiple filenames
    } getOrElse {
      throw new Exception("unable to parse property arguments")
    }
  }
}
