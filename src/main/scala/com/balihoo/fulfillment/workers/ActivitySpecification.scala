package com.balihoo.fulfillment.workers

import com.github.fge.jsonschema.core.report.ProcessingMessage

import scala.collection.mutable

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.fge.jsonschema.main.{JsonSchema, JsonSchemaFactory}

//play imports
import play.api.libs.json._

import scala.language.implicitConversions
import collection.JavaConversions._

case class ActivitySpecificationException(msg: String) extends Exception(msg)

class ActivitySpecification(val params:List[ActivityParameter]
                           ,val result:ActivityResultType
                           ,val description:String = "") {

  val paramsMap:Map[String,ActivityParameter] = (for(param <- params) yield param.name -> param).toMap

  private val __factory = JsonSchemaFactory.byDefault()
  private val __mapper = new ObjectMapper()
  private val __schema:JsonSchema = __factory.getJsonSchema(jsValue2JsNode(parameterSchema))

  private def jsValue2JsNode(jsValue: JsValue) = __mapper.readTree(Json.stringify(jsValue))

  def getSpecification:JsValue = {
    Json.obj(
      "description" -> Json.toJson(description),
      "params" -> parameterSchema,
      "result" -> result.toSchema(includeVersion = true)
    )
  }

  override def toString:String = {
    Json.stringify(getSpecification)
  }

  def parameterSchema:JsObject = {
    Json.obj(
      "$schema" ->"http://json-schema.org/draft-04/schema", // Our preferred schema
      "type" -> "object",
      "required" -> (for((pname, param) <- paramsMap if param.required) yield pname),
      "properties" ->  (for((pname, param) <- paramsMap) yield pname -> param.toSchema)
    )
  }

  def validate(js:JsValue) = {
    val report = __schema.validate(jsValue2JsNode(js))

    report.isSuccess match {
      case false =>
        val gripes = mutable.MutableList[String]()
        for(m:ProcessingMessage <- report) {
          val report = Json.toJson(m.asJson).as[JsObject]
          val domain = report.value("domain").as[String]
          val level = report.value("level").as[String]
          val pointer = report.value("instance").as[JsObject].value("pointer").as[String]
          val message = report.value("message").as[String]
          gripes += s"$domain $level: $pointer $message"
        }
        throw ActivitySpecificationException(gripes.mkString("\n"))
      case _ =>
    }
  }

  def getArgs(input:String):ActivityArgs = {
    getArgs(Json.parse(input).as[JsObject])
  }

  /**
   * Processes the incoming JsObject against the specification. Validates and returns
   * the arguments
   * @param inputObject JsObject
   * @return ActivityArgs
   */
  def getArgs(inputObject:JsObject):ActivityArgs = {
    validate(inputObject)

    val foundParams = mutable.Map[String, Any]()
    for((name, value) <- inputObject.fields) {
      if(paramsMap contains name) {
        val param = paramsMap(name)
        foundParams(name) = param.parseValue(value)
      }
    }

    new ActivityArgs(foundParams.toMap, Json.stringify(inputObject))
  }

  def createResult[T](o: T)(implicit tjs: Writes[T]): ActivityResult = result.jsToResult(tjs.writes(o))
}


class ActivityArgs(val params:Map[String,Any], val input:String = "{}") {

  def has(param:String):Boolean = {
    params contains param
  }

  def apply[T](param:String):T = {
    params(param).asInstanceOf[T]
  }

  def getOrElse[T](param:String, default:T):T = {
    if(has(param)) {
      return params(param).asInstanceOf[T]
    }
    default
  }


  def get[T](param: String): Option[T] = params.get(param).asInstanceOf[Option[T]]

  override def toString:String = {
    params.toString()
  }
}

object ActivityArgs {
  def apply(params: (String, Any)*) = new ActivityArgs(params.toMap)
}
