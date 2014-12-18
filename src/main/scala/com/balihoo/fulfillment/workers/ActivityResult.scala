package com.balihoo.fulfillment.workers

import scala.collection.mutable

import com.github.fge.jsonschema.core.report.ProcessingMessage
import com.github.fge.jsonschema.main.{JsonSchema, JsonSchemaFactory}
import org.keyczar.Crypter
import play.api.libs.json.Json.JsValueWrapper
import com.fasterxml.jackson.databind.ObjectMapper

//play imports
import play.api.libs.json._

import scala.language.implicitConversions
import collection.JavaConversions._

class ActivityResult(val result:JsValue) {
  def serialize():String = Json.stringify(result)
}

class EncryptedResult(result:JsString)
  extends ActivityResult(result) {
  val crypter = new Crypter("config/crypto")

  override def serialize():String = {
    crypter.encrypt(super.serialize())
  }
}

case class ActivityResultException(msg: String) extends Exception(msg)

abstract class ActivityResultType(val description:String) {
  def jsonType:String

  /**
   * This can be overriden to provide additional schema values
   */
  def additionalSchemaValues: Map[String, JsValueWrapper] = Map()

  final def toSchema(includeVersion:Boolean = false):JsValue = {
    val schema = collection.mutable.Map[String, JsValue]()
    schema("type") = Json.toJson(jsonType)
    schema("description") = Json.toJson(description)
    if(includeVersion) {
      schema("$schema") = Json.toJson("http://json-schema.org/draft-04/schema")
    }

    Json.toJson(schema.toMap).as[JsObject] ++ Json.obj(additionalSchemaValues.toList:_*)
  }

  private def jsValue2JsNode(jsValue: JsValue) = __mapper.readTree(Json.stringify(jsValue))
  private val __factory = JsonSchemaFactory.byDefault()
  private val __mapper = new ObjectMapper()
  private val __schema:JsonSchema = __factory.getJsonSchema(jsValue2JsNode(toSchema(includeVersion = true)))

  protected def _jsToResult(js:JsValue):ActivityResult = new ActivityResult(js)

  // We verify that the value we're attempting to return matches what we've declared
  // in the spec.
  final def jsToResult(js:JsValue):ActivityResult = {
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
        throw ActivityResultException(gripes.mkString("\n"))
      case _ =>
        _jsToResult(js)
    }
  }

}

class EncryptedResultType(override val description:String)
  extends ActivityResultType(description + " ENCRYPTED") {
  def jsonType = "string"

  override def _jsToResult(js:JsValue):ActivityResult
    = new EncryptedResult(js.asOpt[JsString].getOrElse(throw new Exception("Encrypted result must be a STRING")))
}

class StringResultType(override val description:String)
  extends ActivityResultType(description) {
  def jsonType = "string"
}

class StringsResultType(override val description:String)
  extends ActivityResultType(description) {
  def jsonType = "array"

  override def additionalSchemaValues = Map("items" -> Json.obj("type" -> "string"))
}

class IntegerResultType(override val description:String)
  extends ActivityResultType(description) {
  def jsonType = "integer"
}

class NumberResultType(override val description:String)
  extends ActivityResultType(description) {
  def jsonType = "number"
}

class DateTimeResultType(override val description:String)
  extends ActivityResultType(description) {
  def jsonType = "string"
  override def additionalSchemaValues = Map("format" -> Json.toJson("date-time"))
}

class ObjectResultType(override val description:String
                       ,val properties:Map[String, ActivityResultType] = Map()
                        )
  extends ActivityResultType(description) {
  def jsonType = "object"

  override def additionalSchemaValues = Map(
    "properties" -> Json.toJson((for((name, property) <- properties) yield name -> property.toSchema()).toMap))
}

class ArrayResultType(override val description:String
                      ,val elementType:ActivityResultType
                       )
  extends ActivityResultType(description) {
  def jsonType = "array"

  override def additionalSchemaValues = Map("items" -> elementType.toSchema())
}

