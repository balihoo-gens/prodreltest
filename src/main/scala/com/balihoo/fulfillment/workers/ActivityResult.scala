package com.balihoo.fulfillment.workers

import org.keyczar.Crypter

//play imports
import play.api.libs.json._

import scala.language.implicitConversions

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


abstract class ActivityResultType(val description:String) {
  def jsonType:String

  /**
   * This can be overriden to provide additional schema values
   */
  def additionalSchemaValues: Map[String, JsValue] = Map()

  final def toSchema(includeVersion:Boolean = false):JsValue = {
    val schema = collection.mutable.Map[String, JsValue]()
    schema("type") = Json.toJson(jsonType)
    schema("description") = Json.toJson(description)
    if(includeVersion) {
      schema("$schema") = Json.toJson("http://json-schema.org/draft-04/schema")
    }

    Json.toJson(schema.toMap).as[JsObject] ++ Json.obj(additionalSchemaValues.mapValues(Json.toJsFieldJsValueWrapper(_)).toSeq:_*)
  }

  // TODO FIXME validate the outgoing result value with what's declared in the spec!!!
  def jsToResult(js:JsValue):ActivityResult = new ActivityResult(js)

}

class EncryptedResultType(override val description:String)
  extends ActivityResultType(description + " ENCRYPTED") {
  def jsonType = "string"

  override def jsToResult(js:JsValue):ActivityResult
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

