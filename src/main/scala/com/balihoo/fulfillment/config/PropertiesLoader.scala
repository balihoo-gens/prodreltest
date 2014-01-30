package com.balihoo.fulfillment.config

import java.util.Properties
import java.io.{FileInputStream, InputStream}

class PropertiesLoader(propertiesFileName: String) {
  private val prop: Properties = new Properties()
  private var propertiesFileStream: InputStream = null
  try {
    propertiesFileStream = new FileInputStream(propertiesFileName)
    prop.load(propertiesFileStream)
  } finally {
    if (propertiesFileStream != null)
      propertiesFileStream.close()
  }

  def getInt(propName: String) : Int = {
    val propVal: String = prop.getProperty(propName)
    if (propVal == null)
      throw new Exception("required config value " + propName + " not found or blank")
    Integer.parseInt(propVal)
  }

  def getString(propName: String) : String = {
    val propVal: String = prop.getProperty(propName)
    if (propVal == null)
      throw new Exception("required config value " + propName + " not found or blank")
    propVal
  }
}
