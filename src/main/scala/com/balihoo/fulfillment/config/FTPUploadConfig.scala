package com.balihoo.fulfillment.config

import com.balihoo.fulfillment.workers.{ActivityResult, ActivityParameter, ActivitySpecification, ActivityParameters}

trait FTPUploadConfigComponent {
  def ftpUploadConfig: AbstractFTPUploadConfig with PropertiesLoaderComponent
}

abstract class AbstractFTPUploadConfig(params: ActivityParameters) {
  this: PropertiesLoaderComponent =>

  val sourceUrl: String = params("sourceUrl")
  val ftpHost: String = params("ftpHost")
  val ftpPort: Int = params.getOrElse("ftpPort", 21)
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
      new ActivityParameter("sourceUrl", "string", "The URL of the file to be uploaded"),
      new ActivityParameter("ftpHost", "string", "The destination host name"),
      new ActivityParameter("ftpPort", "int", "The destination port number (default = 21)", required = false),
      new ActivityParameter("ftpUsername", "string", "The username for the destination FTP server", sensitive = true),
      new ActivityParameter("ftpPassword", "string", "The password for the destination FTP server", sensitive = true),
      new ActivityParameter("ftpDirectory", "string", "The destination directory (default = \"/\")", required = false),
      new ActivityParameter("ftpFilename", "string", "The destination filename")
    ), new ActivityResult("string", "A status message"))
  }
}
