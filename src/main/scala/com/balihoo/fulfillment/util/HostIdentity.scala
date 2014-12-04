package com.balihoo.fulfillment.util

import scala.sys.process._

object HostIdentity {

  private var _hostName:Option[String] = None

  def getHostAddress:String = {
    _hostName match {
      case Some(name) => name
      case None => {
        if (isEC2Instance) {
          //reduce nesting by iterating over a list
          _hostName = ListOps.iterateUntilSome(
            List(
              "public-hostname",
              "public-ipv4",
              "instance-id",
              "hostname",
              "local-hostname",
              "local-ipv4"
            ),
            getEC2MetaData _
          )
        }
        //still a bunch of nesting
        _hostName match {
          case Some(name) => name
          case None => {
            _hostName = getLocalHostname
            _hostName match {
              case Some(name) => name
              case None => {
                _hostName = Some("noname")
                _hostName.get
              }
            }
          }
        }
      }
    }
  }

  def isEC2Instance:Boolean = {
    sys.env.contains("EC2_HOME") || new java.io.File("/etc/ec2_version").exists
  }

  def getEC2MetaData(id:String): Option[String] = {
    val url = s"http://169.254.169.254/latest/meta-data/$id"
    val aws_ec2_identify = s"curl -s $url --max-time 2 --retry 3"
    val res = aws_ec2_identify.!!
    if (res.isEmpty) None else Some(res)
  }

  def getLocalHostname:Option[String] = {
    var res:String = ""
    try {
      // This might throw an exception if the local DNS doesn't know about the system hostname.
      // At this point we're looking for some kind of identifier. It doesn't have to actually
      // be reachable.
      res = java.net.InetAddress.getLocalHost.getHostName
    } catch {
      case e:Exception =>
        // If all else fails..
        res = "hostname".!!
    }
    if (res.isEmpty) None else Some(res)
  }
}
