package com.balihoo.fulfillment.config

import com.balihoo.fulfillment.workers._

trait FTPUploadConfigComponent {
  def ftpUploadConfig: AbstractFTPUploadConfig with PropertiesLoaderComponent
}

abstract class AbstractFTPUploadConfig(params: ActivityParameters) {
  this: PropertiesLoaderComponent =>

  val sourceUrl: String = params[String]("sourceUrl")
  val ftpHost: String = params[String]("ftpHost")
  val ftpPort: Int = params.getOrElse[Int]("ftpPort", 21)
  val ftpUsername: String = params("ftpUsername")
  val ftpPassword: String = params("FtpPassword")
  val ftpDirectory: String = params.getOrElse("ftpDirectory", "/")
  val ftpFilename: String = params("ftpFilename")
}

class FTPUploadConfig(params: ActivityParameters, cfg: PropertiesLoader)
  extends AbstractFTPUploadConfig(params)
  with PropertiesLoaderComponent {

  def config = cfg
}

object FTPUploadConfig {
  def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new StringActivityParameter("sourceUrl", "The URL of the file to be uploaded"),
      new StringActivityParameter("ftpHost", "The destination host name"),
      new IntegerActivityParameter("ftpPort", "The destination port number (default = 21)", required = false),
      new EncryptedActivityParameter("ftpUsername", "The username for the destination FTP server"),
      new EncryptedActivityParameter("ftpPassword", "The password for the destination FTP server"),
      new StringActivityParameter("ftpDirectory", "The destination directory (default = \"/\")", required = false),
      new StringActivityParameter("ftpFilename", "The destination filename")
    ), new StringActivityResult("A status message"))
  }
}
