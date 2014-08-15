package com.balihoo.fulfillment.config

import com.balihoo.fulfillment.workers.ActivityParameters

trait FTPUploadConfigComponent {
  def ftpUploadConfig: AbstractFTPUploadConfig with PropertiesLoaderComponent
}

abstract class AbstractFTPUploadConfig(params: ActivityParameters) {
  this: PropertiesLoaderComponent =>

  val configKey: String = params.getRequiredParameter("configKey")
  val sourceUrl: String = params.getRequiredParameter("sourceUrl")
  val ftpDirectory: String = params.getOptionalParameter("ftpDirectory", "/")
  val ftpFilename: String = params.getRequiredParameter("ftpFilename")
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
