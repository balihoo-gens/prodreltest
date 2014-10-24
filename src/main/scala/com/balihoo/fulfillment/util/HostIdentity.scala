package com.balihoo.fulfillment.util

import scala.sys.process._
object HostIdentity {

  def getHostAddress:String = {
    new java.io.File("/etc/ec2_version").exists match {
      case true => getEC2Address
      case _ =>
        sys.env.get("EC2_HOME") match {
          case Some(_:String) => getEC2Address
          case None => getHostname
        }
    }
  }

  def getEC2Address:String = {
    val url = "http://169.254.169.254/latest/meta-data/public-hostname"
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
