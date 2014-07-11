package com.balihoo.fulfillment.dashboard

import play.api.libs.json._
import scala.collection.JavaConversions._

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

abstract class DashServlet extends HttpServlet {

  val getHandlers = collection.mutable.Map[String, (DashTransaction) => Unit]()
  val postHandlers = collection.mutable.Map[String, (DashTransaction) => Unit]()
  val putHandlers = collection.mutable.Map[String, (DashTransaction) => Unit]()
  val deleteHandlers = collection.mutable.Map[String, (DashTransaction) => Unit]()

  override protected def doGet(request:HttpServletRequest
                               ,response:HttpServletResponse) = {
    _process(getHandlers, new DashTransaction(request, response))
  }

  override protected def doPost(request:HttpServletRequest
                               ,response:HttpServletResponse) = {
    _process(postHandlers, new DashTransaction(request, response))
  }

  override protected def doPut(request:HttpServletRequest
                               ,response:HttpServletResponse) = {
    _process(putHandlers, new DashTransaction(request, response))
  }

  override protected def doDelete(request:HttpServletRequest
                               ,response:HttpServletResponse) = {
    _process(deleteHandlers, new DashTransaction(request, response))
  }

  def _errorJson(message:String, details:String = ""): String = {
    Json.stringify(Json.toJson(Map(
      "error" -> message,
      "details" -> details
    )))
  }

  def _process(handlers:collection.mutable.Map[String, (DashTransaction) => Unit]
               ,dtrans:DashTransaction) = {

    try {
      handlers(dtrans.request.getRequestURI)(dtrans)
    } catch {
      case bre:BadRequestException =>
        dtrans.respondJson(HttpServletResponse.SC_BAD_REQUEST
          , _errorJson(bre.getMessage))
      case nsee:NoSuchElementException =>
        dtrans.respondJson(HttpServletResponse.SC_NOT_FOUND
          , _errorJson(nsee.getMessage))
      case e:Exception =>
        dtrans.respondJson(HttpServletResponse.SC_INTERNAL_SERVER_ERROR
          ,_errorJson(e.getMessage, e.getClass.toString))
    }
  }

  def get(path:String, code:(DashTransaction) => Unit) = {
    getHandlers(path) = code
  }
  def post(path:String, code:(DashTransaction) => Unit) = {
    postHandlers(path) = code
  }
  def put(path:String, code:(DashTransaction) => Unit) = {
    putHandlers(path) = code
  }
  def delete(path:String, code:(DashTransaction) => Unit) = {
    deleteHandlers(path) = code
  }
}

class BadRequestException(message:String) extends Exception(message)

class DashTransaction(val request:HttpServletRequest
                      ,val response:HttpServletResponse) {

  val params = collection.mutable.Map[String, String]()
  for((key, values) <- request.getParameterMap) {
    params(key) = values(0)
  }

  def hasParameter(param:String) = {
    params contains param
  }

  def getRequiredParameter(param:String):String = {
    if(!(params contains param)) {
      throw new Exception(s"Input parameter '$param' is REQUIRED!")
    }
    params(param)
  }

  def getOptionalParameter(param:String, default:String):String = {
    params.getOrElse(param, default)
  }

  def respondJson(code:Int, json:String) = {
    response.setContentType("application/json")
    response.setStatus(code)
    response.getWriter.println(json)
  }
}
