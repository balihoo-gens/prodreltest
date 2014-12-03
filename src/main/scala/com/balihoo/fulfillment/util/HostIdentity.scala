package com.balihoo.fulfillment.util

import scala.sys.process._
object HostIdentity {

  def getHostAddress:String = {
    new java.io.File("/etc/ec2_version").exists match {
      case true => getEC2Address
      case _ =>
        val name = sys.env.get("EC2_HOME") match {
          case Some(_:String) => {
              val name = getEC2HostName
              if (name.isEmpty) getEC2Address else name
          }
          case None => getHostname
        }
        if (name.isEmpty) "noname" else name
    }
  }

  def getEC2Address:String = {
    getEC2MetaData("public-ipv4")
  }

  def getEC2HostName:String = {
    getEC2MetaData("public-hostname")
  }

  def getEC2MetaData(id:String): String = {
    val url = s"http://169.254.169.254/latest/meta-data/$id"
    val aws_ec2_identify = s"curl -s $url --max-time 2 --retry 3"
    aws_ec2_identify.!!
  }

  def getHostname:String = {
    try {
      // This might throw an exception if the local DNS doesn't know about the system hostname.
      // At this point we're looking for some kind of identifier. It doesn't have to actually
      // be reachable.
      java.net.InetAddress.getLocalHost.getHostName
    } catch {
      case e:Exception =>
        // If all else fails..
        "hostname".!!
    }

  }
}
