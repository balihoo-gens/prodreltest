package com.balihoo.fulfillment.dashboard

import play.api.libs.json._
import scala.collection.JavaConversions._

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

abstract class RestServlet extends HttpServlet {

  val getHandlers = collection.mutable.Map[String, (RestServletQuery) => Unit]()
  val postHandlers = collection.mutable.Map[String, (RestServletQuery) => Unit]()
  val putHandlers = collection.mutable.Map[String, (RestServletQuery) => Unit]()
  val deleteHandlers = collection.mutable.Map[String, (RestServletQuery) => Unit]()

  override protected def doGet(request:HttpServletRequest
                               ,response:HttpServletResponse) = {
    _process(getHandlers, new RestServletQuery(request, response))
  }

  override protected def doPost(request:HttpServletRequest
                               ,response:HttpServletResponse) = {
    _process(postHandlers, new RestServletQuery(request, response))
  }

  override protected def doPut(request:HttpServletRequest
                               ,response:HttpServletResponse) = {
    _process(putHandlers, new RestServletQuery(request, response))
  }

  override protected def doDelete(request:HttpServletRequest
                               ,response:HttpServletResponse) = {
    _process(deleteHandlers, new RestServletQuery(request, response))
  }

  def _errorJson(message:String, details:String = ""): String = {
    Json.stringify(Json.toJson(Map(
      "error" -> message,
      "details" -> details
    )))
  }

  def _process(handlers:collection.mutable.Map[String, (RestServletQuery) => Unit]
               ,rsq:RestServletQuery) = {

    try {
      handlers(rsq.request.getRequestURI)(rsq)
    } catch {
      case bre:BadRequestException =>
        rsq.respondJson(HttpServletResponse.SC_BAD_REQUEST
          , _errorJson(bre.getMessage))
      case nsee:NoSuchElementException =>
        rsq.respondJson(HttpServletResponse.SC_NOT_FOUND
          , _errorJson(nsee.getMessage))
      case npe:NullPointerException =>
//        println(npe.getMessage)
//        npe.printStackTrace()
        rsq.respondJson(HttpServletResponse.SC_INTERNAL_SERVER_ERROR
          ,_errorJson(npe.getMessage))
      case e:Exception =>
//        println(e.getMessage)
//        println(e.getStackTrace)
        rsq.respondJson(HttpServletResponse.SC_INTERNAL_SERVER_ERROR
          ,_errorJson(e.getMessage, e.getClass.toString))
    }
  }

  def get(path:String, code:(RestServletQuery) => Unit) = {
    getHandlers(path) = code
  }
  def post(path:String, code:(RestServletQuery) => Unit) = {
    postHandlers(path) = code
  }
  def put(path:String, code:(RestServletQuery) => Unit) = {
    putHandlers(path) = code
  }
  def delete(path:String, code:(RestServletQuery) => Unit) = {
    deleteHandlers(path) = code
  }
}

class BadRequestException(message:String) extends Exception(message)

class RestServletQuery(val request:HttpServletRequest
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
