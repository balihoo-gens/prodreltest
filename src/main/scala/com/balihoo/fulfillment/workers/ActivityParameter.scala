package com.balihoo.fulfillment.workers

import java.net.URI
import org.joda.time.DateTime
import org.keyczar.Crypter
import play.api.libs.json.Json.JsValueWrapper

//play imports
import play.api.libs.json._
import scala.language.implicitConversions

abstract class ActivityParameter(val name:String
                                 ,val description:String
                                 ,val required:Boolean = true) {

  def jsonType:String

  /**
   * This can be overriden to provide additional schema values
   */
  def additionalSchemaValues: Map[String, JsValueWrapper] = Map()

  def parseValue(js:JsValue):Any

  final def toSchema:JsValue = {
    Json.obj(
      "type" -> jsonType,
      "description" -> description
    ) ++ Json.obj(additionalSchemaValues.toList:_*)
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

class EncryptedParameter(override val name:String
                         ,override val description:String
                         ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  val crypter = new Crypter("config/crypto")
  def jsonType = "string"

  def parseValue(js:JsValue):Any = {
    crypter.decrypt(js.as[String])
  }
}

class StringParameter(override val name:String
                      ,override val description:String
                      ,override val required:Boolean = true
                      ,val maxLength:Option[Int] = None
                      ,val minLength:Option[Int] = None
                      ,val pattern:Option[String] = None)
  extends ActivityParameter(name, description, required) {

  def jsonType = "string"

  override def additionalSchemaValues: Map[String, JsValueWrapper] = {
    val schema = collection.mutable.Map[String, JsValueWrapper]()

    if(maxLength.nonEmpty) schema("maxLength") = Json.toJson[Int](maxLength.get)
    if(minLength.nonEmpty) schema("minLength") = Json.toJson[Int](minLength.get)
    if(pattern.nonEmpty) schema("pattern") = Json.toJson(pattern.get)

    schema.toMap
  }

  def parseValue(js:JsValue):Any = _parseBasic[String](js)
}

class IntegerParameter(override val name:String
                       ,override val description:String
                       ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  override def jsonType = "integer"

  override def parseValue(js:JsValue):Any = _parseBasic[Int](js)
}

class LongParameter(override val name:String
                    ,override val description:String
                    ,override val required:Boolean = true) extends IntegerParameter(name, description, required) {
  override def parseValue(js:JsValue):Any = _parseBasic[Long](js)
}

class NumberParameter(override val name:String
                      ,override val description:String
                      ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "number"

  def parseValue(js:JsValue):Any = _parseBasic[Double](js)
}

class BooleanParameter(override val name:String
                       ,override val description:String
                       ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "boolean"

  def parseValue(js:JsValue):Any = _parseBasic[Boolean](js)
}

class StringsParameter(override val name:String
                       ,override val description:String
                       ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "array"

  override def additionalSchemaValues = Map("items" -> Json.obj("type" -> "string"))

  def parseValue(js: JsValue): Any = _parseBasic[List[String]](js)
}

class EnumsParameter(override val name:String
                     ,override val description:String
                     ,val options:Seq[String]
                     ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "array"

  def parseValue(js:JsValue):Any = _parseBasic[List[String]](js)

  override def additionalSchemaValues =
    Map("items" ->
      Json.obj(
        "type" -> "string",
        "enum" -> Json.toJson(options)
      )
    )
}

class ObjectParameter(override val name:String
                      ,override val description:String
                      ,val properties:List[ActivityParameter] = List()
                      ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  val propsMap:Map[String,ActivityParameter] = (for(prop <- properties) yield prop.name -> prop).toMap
  val requiredProperties = for(prop <- properties if prop.required) yield prop.name
  def jsonType = "object"

  override def additionalSchemaValues = {
    val schema = collection.mutable.Map[String, JsValueWrapper]()
    if(requiredProperties.size > 0) {
      // Schema gets upset if you have "required" with an empty list. so we omit it completely if there are
      // no required properties
      schema("required") = Json.toJson(requiredProperties)
    }

    schema("properties") = Json.toJson((for(property <- properties) yield property.name -> property.toSchema).toMap)

    schema.toMap
  }

  def parseValue(js:JsValue):Any = {
    new ActivityArgs(
      (for((name, property) <- js.as[JsObject].fields)
      yield name -> (if(propsMap contains name) propsMap(name).parseValue(property) else property)).toMap
      , Json.stringify(js))
  }
}

class ArrayParameter(override val name:String
                     ,override val description:String
                     ,val element:ActivityParameter
                     ,override val required:Boolean = true
                     ,val minItems: Int = 0
                     , val uniqueItems: Boolean = false)
  extends ActivityParameter(name, description, required) {

  def jsonType = "array"

  override def additionalSchemaValues = {
    val schema = collection.mutable.Map[String, JsValueWrapper]()
    schema("items") = element.toSchema
    if (minItems > 0) schema("minItems") = Json.toJson(minItems)
    if (uniqueItems) schema("uniqueItems") = Json.toJson(uniqueItems)

    schema.toMap
  }

  def parseValue(js:JsValue):Any = (for(item <- js.as[JsArray].value) yield element.parseValue(item)).toList
}

class EnumParameter(override val name:String
                    ,override val description:String
                    ,val options:Seq[String]
                    ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "string"

  override def additionalSchemaValues = Map("enum" -> Json.toJson(options))

  def parseValue(js:JsValue):Any = _parseBasic[String](js)
}

class DateTimeParameter(override val name:String
                        ,override val description:String
                        ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "string"

  override def additionalSchemaValues = Map("format" -> Json.toJson("date-time"))

  def parseValue(js: JsValue): Any = new DateTime(_parseBasic[String](js))
}

class UriParameter(override val name:String
                   ,override val description:String
                   ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "string"

  override def additionalSchemaValues = Map("format" -> Json.toJson("uri"), "minLength" -> Json.toJson(1))

  def parseValue(js: JsValue): Any = new URI(_parseBasic[String](js).toString)
}

class EmailParameter(override val name:String
                     ,override val description:String
                     ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "string"

  override def additionalSchemaValues = Map("format" -> Json.toJson("email"))

  def parseValue(js: JsValue): Any = _parseBasic[String](js)
}

class Ipv4Parameter(override val name:String
                    ,override val description:String
                    ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "string"

  override def additionalSchemaValues = Map("format" -> Json.toJson("ipv4"))

  def parseValue(js: JsValue): Any = _parseBasic[String](js)
}

class Ipv6Parameter(override val name:String
                    ,override val description:String
                    ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "string"

  override def additionalSchemaValues = Map("format" -> Json.toJson("ipv6"))

  def parseValue(js: JsValue): Any = _parseBasic[String](js)
}

class HostnameParameter(override val name:String
                        ,override val description:String
                        ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "string"

  override def additionalSchemaValues = Map("format" -> Json.toJson("hostname"))

  def parseValue(js: JsValue): Any = _parseBasic[String](js)
}

class StringMapParameter(override val name:String
                                 ,override val description:String
                                 ,override val required:Boolean = true)
  extends ActivityParameter(name, description, required) {

  def jsonType = "object"

  def parseValue(js:JsValue):Any = _parseBasic[Map[String, String]](js)
}

