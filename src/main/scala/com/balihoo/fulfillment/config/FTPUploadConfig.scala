package com.balihoo.fulfillment.config

import com.balihoo.fulfillment.workers.{ActivityResult, ActivityParameter, ActivitySpecification, ActivityParameters}

trait FTPUploadConfigComponent {
  def ftpUploadConfig: AbstractFTPUploadConfig with PropertiesLoaderComponent
}

abstract class AbstractFTPUploadConfig(params: ActivityParameters) {
  this: PropertiesLoaderComponent =>

  val configKey: String = params("configKey")
  val sourceUrl: String = params("sourceUrl")
  val ftpDirectory: String = params.getOrElse("ftpDirectory", "/")
  val ftpFilename: String = params("ftpFilename")
  val ftpHost: String = config.getString(configKey + "FtpHost")
  val ftpPort: Int = config.getOptInt(configKey + "FtpPort", 21)
  val ftpUsername: String = config.getString(configKey + "FtpUsername")
  val ftpPassword: String = config.getString(configKey + "FtpPassword")
}

class FTPUploadConfig(params: ActivityParameters, cfg: PropertiesLoader)
  extends AbstractFTPUploadConfig(params)
  with PropertiesLoaderComponent {

  def config = cfg
}

object FTPUploadConfig {
  def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ActivityParameter("configKey", "string", "Used to look up the destination FTP server and credentials."),
      new ActivityParameter("sourceUrl", "string", "The URL of the file to be uploaded"),
      new ActivityParameter("ftpDirectory", "string", "The destination directory (default = \"/\")", false),
      new ActivityParameter("ftpFilename", "string", "The destination filename")
    ), new ActivityResult("string", "A status message"))
  }
}
