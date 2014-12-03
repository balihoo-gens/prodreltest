package com.balihoo.fulfillment.workers

import java.net.URI

import com.github.fge.jsonschema.core.report.ProcessingMessage
import org.joda.time.DateTime

import scala.collection.mutable

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.fge.jsonschema.main.{JsonSchema, JsonSchemaFactory}

import org.keyczar.Crypter

//play imports
import play.api.libs.json._

import scala.language.implicitConversions
import collection.JavaConversions._

class ActivitySpecification(val params:List[ActivityParameter]
                           ,val result:ActivityResult
                           ,val description:String = "") {

  val paramsMap:Map[String,ActivityParameter] = (for(param <- params) yield param.name -> param).toMap

  private val __factory = JsonSchemaFactory.byDefault()
  private val mapper = new ObjectMapper()
  private val __schema:JsonSchema = __factory.getJsonSchema(jsValue2JsNode(parameterSchema))

  private def jsValue2JsNode(jsValue: JsValue) = mapper.readTree(Json.stringify(jsValue))

  def getSpecification:JsValue = {
    Json.obj(
      "result" -> result.toJson,
      "description" -> Json.toJson(description),
      "schema" -> parameterSchema
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
        throw new Exception(gripes.mkString("\n"))
      case _ =>
    }
  }

  def getParameters(input:String):ActivityParameters = {
    getParameters(Json.parse(input).as[JsObject])
  }

  def getParameters(inputObject:JsObject):ActivityParameters = {
    validate(inputObject)

    val foundParams = mutable.Map[String, Any]()
    for((name, value) <- inputObject.fields) {
      if(paramsMap contains name) {
        val param = paramsMap(name)
        foundParams(name) = param.parseValue(value)
      }
    }

    new ActivityParameters(foundParams.toMap, Json.stringify(inputObject))
  }

}

abstract class ActivityResult(val description:String) {
  def jsonType:String

  def toJson:JsValue = {
    Json.toJson(Map(
      "type" -> Json.toJson(jsonType),
      "description" -> Json.toJson(description)
    ))
  }

  def toSchema:JsValue = {
    Json.obj(
      "type" -> jsonType,
        "description" -> description
    )
  }
}

class EncryptedActivityResult(override val description:String)
  extends ActivityResult(description + " ENCRYPTED") {
  def jsonType = "string"
}

class StringActivityResult(override val description:String)
  extends ActivityResult(description) {
  def jsonType = "string"
}

class StringsActivityResult(override val description:String)
  extends ActivityResult(description) {
  def jsonType = "array"

  override def toSchema:JsValue = {
    Json.obj(
      "type" -> jsonType,
      "description" -> description,
      "items" -> Json.obj(
        "type" -> "string"
      )
    )
  }
}

class ObjectActivityResult(override val description:String)
  extends ActivityResult(description) {
  def jsonType = "object"

  override def toSchema:JsValue = {
    Json.obj(
      "type" -> jsonType,
      "description" -> description,
      "properties" -> Json.obj()
    )
  }
}

abstract class ActivityParameter(val name:String
                                 ,val description:String
                                 ,val required:Boolean = true) {

  def jsonType:String

  /**
   * This can be overriden to provide additional schema values
   */
  def additionalSchemaValues: Map[String, JsValue] = Map()

  def parseValue(js:JsValue):Any

  final def toJson:JsValue = {
    Json.toJson(Map(
      "name" -> Json.toJson(name),
      "type" -> Json.toJson(jsonType),
      "description" -> Json.toJson(description),
      "required" -> Json.toJson(required)
    ) ++ additionalSchemaValues)
  }

  final def toSchema:JsValue = {
    Json.obj(
      "type" -> jsonType,
      "description" -> description
    ) ++ Json.obj(additionalSchemaValues.mapValues(Json.toJsFieldJsValueWrapper(_)).toSeq:_*)
  }

  // This one works for anything that is automatically/implicitly convertible via Json.validate
  protected def _parseBasic[T: Reads](js:JsValue):Any = {
    js.validate[T] match {
      case s:JsSuccess[T] =>
        s.get
      case _ =>
        throw new Exception(s"Expected $jsonType but got '${Json.stringify(js)}'!")
    }
  }

}

class EncryptedActivityParameter(override val name:String
                              ,override val description:String
                              ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  val crypter = new Crypter("config/crypto")
  def jsonType = "string"

  def parseValue(js:JsValue):Any = {
    crypter.decrypt(js.as[String])
  }
}

class StringActivityParameter(override val name:String
                              ,override val description:String
                              ,override val required:Boolean = true
                              ,val maxLength:Option[Int] = None
                              ,val minLength:Option[Int] = None
                              ,val pattern:Option[String] = None)
  extends ActivityParameter(name, description, required) {

  def jsonType = "string"

  override def additionalSchemaValues: Map[String, JsValue] = {
    (maxLength.nonEmpty match {
      case true => Map("maxLength" -> Json.toJson[Int](maxLength.get))
      case _ => Map[String, JsValue]()
    }) ++
      (minLength.nonEmpty match {
      case true => Map("minLength" -> Json.toJson[Int](minLength.get))
      case _ => Map()
    }) ++
      (pattern.nonEmpty match {
      case true => Map("pattern" -> Json.toJson(pattern.get))
      case _ => Map()
    })
  }

  def parseValue(js:JsValue):Any = _parseBasic[String](js)
}

class IntegerActivityParameter(override val name:String
                               ,override val description:String
                               ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  override def jsonType = "integer"

  override def parseValue(js:JsValue):Any = _parseBasic[Int](js)
}

class LongActivityParameter(override val name:String
                            ,override val description:String
                            ,override val required:Boolean = true) extends IntegerActivityParameter(name, description, required) {
  override def parseValue(js:JsValue):Any = _parseBasic[Long](js)
}

class NumberActivityParameter(override val name:String
                              ,override val description:String
                              ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "number"

  def parseValue(js:JsValue):Any = _parseBasic[Double](js)
}

class BooleanActivityParameter(override val name:String
                               ,override val description:String
                               ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "boolean"

  def parseValue(js:JsValue):Any = _parseBasic[Boolean](js)
}

class StringsActivityParameter(override val name:String
                               ,override val description:String
                               ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "array"

  override def additionalSchemaValues = Map("items" -> Json.obj("type" -> "string"))

  def parseValue(js: JsValue): Any = _parseBasic[List[String]](js)
}

class EnumsActivityParameter(override val name:String
                               ,override val description:String
                               ,val options:Seq[String]
                               ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "array"

  def parseValue(js:JsValue):Any = _parseBasic[List[String]](js)

  override def additionalSchemaValues =
    Map("items" -> Json.obj(
          "type" -> "string",
          "enum" -> Json.toJson(options)
        )
      )
}

class ObjectActivityParameter(override val name:String
                              ,override val description:String
                              ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "object"

  override def additionalSchemaValues = Map("properties" -> Json.obj())

  def parseValue(js:JsValue):Any = js.as[JsObject]
}

class EnumActivityParameter(override val name:String
                            ,override val description:String
                            ,val options:Seq[String]
                            ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "string"

  override def additionalSchemaValues = Map("enum" -> Json.toJson(options))

  def parseValue(js:JsValue):Any = _parseBasic[String](js)
}

class DateTimeActivityParameter(override val name:String
                                ,override val description:String
                                ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "string"

  override def additionalSchemaValues = Map("format" -> Json.toJson("date-time"))

  def parseValue(js: JsValue): Any = new DateTime(_parseBasic[String](js))
}

class UriActivityParameter(override val name:String
                                ,override val description:String
                                ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "string"

  override def additionalSchemaValues = Map("format" -> Json.toJson("uri"))

  def parseValue(js: JsValue): Any = new URI(_parseBasic[String](js).toString)
}

class EmailActivityParameter(override val name:String
                            ,override val description:String
                            ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "string"

  override def additionalSchemaValues = Map("format" -> Json.toJson("email"))

  def parseValue(js: JsValue): Any = _parseBasic[String](js)
}

class Ipv4ActivityParameter(override val name:String
                           ,override val description:String
                           ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "string"

  override def additionalSchemaValues = Map("format" -> Json.toJson("ipv4"))

  def parseValue(js: JsValue): Any = _parseBasic[String](js)
}

class Ipv6ActivityParameter(override val name:String
                            ,override val description:String
                            ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "string"

  override def additionalSchemaValues = Map("format" -> Json.toJson("ipv6"))

  def parseValue(js: JsValue): Any = _parseBasic[String](js)
}

class HostnameActivityParameter(override val name:String
                            ,override val description:String
                            ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "string"

  override def additionalSchemaValues = Map("format" -> Json.toJson("hostname"))

  def parseValue(js: JsValue): Any = _parseBasic[String](js)
}


class ActivityParameters(val params:Map[String,Any], val input:String = "{}") {

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
