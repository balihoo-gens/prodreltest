package com.balihoo.fulfillment.deciders

import java.net.URLEncoder
import java.security.MessageDigest

import org.keyczar.exceptions.KeyNotFoundException
import play.api.libs.json._
import org.keyczar.Crypter

import scala.util.matching.Regex
import scala.collection.mutable

object JsonOpName extends Enumeration {
  val STRING_FORMAT = Value("stringformat")
  val URL_ENCODE = Value("urlencode")
  val MD5 = Value("md5")
  val JSON_PATH = Value("jsonpath")
  val OBJECT_KEYS = Value("objectkeys")
  val OBJECT_VALUES = Value("objectvalues")
  val STRING_CONCAT = Value("stringconcat")
//  val ARRAY_UNION = Value("arrayunion")
//  val ARRAY_DIFF = Value("arraydiff")
//  val ARRAY_INTERSECT = Value("arrayintersect")
  //  val SECTION = Value("Section") // NOT ACTUALLY AN OPERATOR!
}

class JsonOp(val name:JsonOpName.Value, val specification:JsonOpSpec, code:(JsonOpArgs) => JsValue) {

  def apply(inputParams:JsValue):JsValue = {
    code(specification.getArgs(inputParams))
  }
}

class JsonOpResult(val rtype:String, description:String, val sensitive:Boolean = false) {
  def toJson:JsValue = {
    Json.obj(
      "type" -> rtype,
      "description" -> description,
      "sensitive" -> sensitive
    )
  }
}

class JsonOpParameter(val name:String, val ptype:String, val description:String, val required:Boolean = true, val sensitive:Boolean = false) {
  var value:Option[JsValue] = None

  def toJson:JsValue = {
    Json.obj(
      "name" -> name,
      "type" -> ptype,
      "description" -> description,
      "required" -> required,
      "sensitive" -> sensitive
    )
  }
}

class JsonOpArgs(val kwargs:Map[String,JsValue], val args:Seq[JsValue], val input:String = "{}") {

  def has(param:String):Boolean = {
    kwargs contains param
  }

  def hasIndex(i:Int):Boolean = {
    args contains i
  }

  def apply(index:Int):JsValue = {
    if(args.size <= index) {
      throw new IndexOutOfBoundsException(s"Index $index not found!")
    }
    args(index)
  }

  def apply[T](index:Int)(implicit r: Reads[T]):T = {
    if(args.size <= index) {
      throw new IndexOutOfBoundsException(s"Index $index not found!")
    }
    args(index).validate[T] match {
      case s: JsSuccess[T] => s.get
      case _ =>
        throw new Exception("Parameter of type:"/*+m.toString()*/+" does not exist at index "+index)
    }
  }

  def apply[T](param:String)(implicit r: Reads[T]):T = {
    if(!(kwargs contains param)) {
      throw new KeyNotFoundException(s"Key $param not found!".getBytes)
    }
    kwargs(param).validate[T] match {
      case s: JsSuccess[T] => s.get
      case _ =>
        throw new Exception("Parameter of type:"/*+m.toString()*/+" does not exist for name "+param)
    }
  }

  def slice(from:Int, until:Int):Seq[JsValue] = {
    args.slice(from, until)
  }

  def argsAs[T]()(implicit r: Reads[T]):Seq[T] = {
    for(a <- args) yield a.as[T]
  }

  def getOrElse[T](param:String, default:T)(implicit m: Reads[T]):T = {
    if(has(param)) {
      return apply[T](param)
    }
    default
  }

  override def toString:String = {
    kwargs.toString() + args.toString()
  }
}

class JsonOpSpec(val description:String, val params:List[JsonOpParameter], val result:JsonOpResult) {

  val crypter = new Crypter("config/crypto")
  val paramsMap:Map[String,JsonOpParameter] = (for(param <- params) yield param.name -> param).toMap

  def toJson:JsValue = {
    Json.obj(
      "description" -> description,
      "parameters" -> Json.toJson((for(param <- params) yield param.name -> param.toJson).toMap),
      "result" -> result.toJson
    )
  }

  override def toString:String = {
    Json.stringify(toJson)
  }

  def getArgs(inputParams:JsValue):JsonOpArgs = {
    inputParams match {
      case jObj: JsObject => _processObject(jObj)
      case jArr: JsArray => _processArray(jArr)
      case _ =>
        new JsonOpArgs(
          Map()
          ,List(inputParams)
          ,Json.stringify(inputParams))

    }
  }

  def _processArray(jArr:JsArray):JsonOpArgs = {

    new JsonOpArgs(
      Map()
      ,jArr.value
      ,Json.stringify(jArr))
  }

  def _processObject(jObj:JsObject):JsonOpArgs = {
    val undeclaredMap = mutable.Map[String, JsValue]()
    val declaredMap = mutable.Map[String, JsValue]()
    for((name, value) <- jObj.fields) {
      if(paramsMap contains name) {
        val param = paramsMap(name)
        declaredMap(name) =
          if(param.sensitive)
            value match {
              case s:JsString =>
                Json.toJson(crypter.decrypt(s.value))
              case _ =>
                throw new Exception("Only strings can be decrypted!")
            }
          else
            value

      } else {
        undeclaredMap(name) = value
      }
    }
    for(param <- params) {
      if(param.required && !(declaredMap contains param.name)) {
        throw new Exception(s"input parameter '${param.name}' is REQUIRED!")
      }
    }

    new JsonOpArgs(
      declaredMap.toMap ++ undeclaredMap.toMap
      ,List()
      ,Json.stringify(jObj))
  }

}

object JsonOps {

  protected val md5Operator =
    new JsonOp(
      JsonOpName.MD5,
      new JsonOpSpec("Computes an MD5 hash of the given value",
        List(),
        new JsonOpResult("string", "MD5 checksum of 'input'")
      ),
      (args) => {
        Json.toJson(MessageDigest.getInstance("MD5").digest(args[String](0).getBytes).map("%02X".format(_)).mkString)
      })

  protected val stringFormatOperator =
    new JsonOp(
      JsonOpName.STRING_FORMAT,
      new JsonOpSpec("Injects strings into the provided 'format' string",
        List(new JsonOpParameter("format", "string", "A string containing {param1} {param2}.. tokens")
          ,new JsonOpParameter("...", "string", "Strings to be substituted into 'format' at token locations", false)
        ),
        new JsonOpResult("string", "'format' with tokens substituted")
      ),
      (args) => {
        val pattern = new Regex("""\{(\w+)\}""", "token")
        Json.toJson(pattern.replaceAllIn(
          _escapeDollar(args[String]("format")),
          m => _escapeDollar(args.getOrElse[String](m.group("token"), "--"))
        ))
      })

  protected val jsonPathOperator =
    new JsonOp(
      JsonOpName.JSON_PATH,
      new JsonOpSpec("Fetches elements from JSON at the provided path",
        List(new JsonOpParameter("path", "string", "")
          ,new JsonOpParameter("json", "JSON", "")
        ),
        new JsonOpResult("JSON", "the element found 'path'")
      ),
      (args) => {
        val path = new ReferencePath(args[String]("path"))
        Json.toJson(path.getValue(args[JsValue]("json")))
      })

  protected val urlEncodeOperator =
    new JsonOp(
      JsonOpName.URL_ENCODE,
      new JsonOpSpec("URL Encodes the given value",
        List(),
        new JsonOpResult("string", "URLEncoded form of 'input'")
      ),
      (args) => {
        Json.toJson(URLEncoder.encode(args[String](0), "UTF-8"))
      })

  protected val objectKeysOperator =
    new JsonOp(
      JsonOpName.OBJECT_KEYS,
      new JsonOpSpec("Returns the keys of an object",
        List(),
        new JsonOpResult("Array[String]", "")
      ),
      (args) => {
        Json.toJson(for((k, v) <- args.kwargs) yield k)
      })

  protected val objectValuesOperator =
    new JsonOp(
      JsonOpName.OBJECT_VALUES,
      new JsonOpSpec("Returns the values of an object",
        List(),
        new JsonOpResult("Array[]", "")
      ),
      (args) => {
        Json.toJson(for((k, v) <- args.kwargs) yield v)
      })

  protected val operators = Map[JsonOpName.Value, JsonOp](
    md5Operator.name -> md5Operator,
    stringFormatOperator.name -> stringFormatOperator,
    urlEncodeOperator.name -> urlEncodeOperator,
    objectKeysOperator.name -> objectKeysOperator,
    objectValuesOperator.name -> objectValuesOperator,
    jsonPathOperator.name -> jsonPathOperator
  )

  protected def _escapeDollar(s:String):String = {
    s.replaceAllLiterally("$", """\$""")
  }

  def apply(operator:JsonOpName.Value, params:JsValue):JsValue = {
    if(!(operators.keySet contains operator)) {
      throw new Exception(s"Operator '${operator.toString}' has no implementation!")
    }
    try {
      operators(operator)(params)
    } catch {
      case e:Exception =>
        throw new Exception(s"Problem processing operator <(${operator.toString})> with input '${Json.stringify(params) take 150}...' :"+e.toString)
    }
  }

  def toJson:JsValue = {
    Json.toJson((for((name, operator) <- operators) yield name.toString -> operator.specification.toJson).toMap)
  }
}

class ReferencePathComponent(val key:Option[String] = None, val index:Option[Int] = None) {

  if((key.isEmpty && index.isEmpty) || (key.isDefined && index.isDefined))
    throw new Exception("PathComponent must have a key xor index!") // Exclusive OR

  override def toString = {
    Json.stringify(toJson)
  }

  def toJson: JsValue = {
    val obj = mutable.Map[String, String]()
    if(key.isDefined) {
      obj("key") = key.get
    } else {
      obj("index") = index.get.toString
    }
    Json.toJson(obj.toMap)
  }
}

object ReferencePath {

  def isJsonPath(candidate:String):Boolean = {
    candidate.matches(".*[/\\[\\]]+.*")
  }
}

class ReferencePath(path:String) {

  private var components = mutable.MutableList[ReferencePathComponent]()
  private val pattern = new Regex("""([^\[\]/]+)|(\[\d+\])""")
  private val matches = (for(m <- pattern.findAllIn(path)) yield m).toList

  for(part <- matches) {
    components += (part contains "[" match {
      case true =>
        new ReferencePathComponent(None, Some(part.substring(1, part.length - 1).toInt))
      case _ =>
        new ReferencePathComponent(Some(part), None)
    })
  }

  def getValue(jsVal:JsValue):JsValue = {
    var current:JsValue = jsVal
    for(component <- components) {
      component.key.isDefined match {
        case true =>
          current match {
            case jObj:JsObject =>
              current = jObj.value(component.key.get)
            case _ =>
              throw new Exception(s"Expected JSON Object with key '${component.key.get}'!")
          }
        case _ =>
          current match {
            case jArr:JsArray =>
              val l = jArr.as[List[JsValue]]
              current = l(component.index.get)
            case _ =>
              throw new Exception(s"Expected JSON Array to index with ${component.index.get}!")

          }
      }
    }

    current
  }

  def popFront():ReferencePathComponent = {
    val head = components.head
    components = components.slice(1, components.length)
    head
  }

  def getComponent(i:Int):ReferencePathComponent = {
    components(i)
  }

  override def toString = {
    Json.stringify(toJson)
  }

  def toJson: JsValue = {
    Json.toJson(for(component <- components) yield component.toJson)
  }
}


