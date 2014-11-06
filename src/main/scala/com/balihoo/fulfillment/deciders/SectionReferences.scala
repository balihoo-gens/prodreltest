package com.balihoo.fulfillment.deciders

import play.api.libs.json._

import scala.collection.mutable
import org.joda.time.DateTime
import scala.util.matching.Regex

class SectionReferences(sectionNames:Seq[String], fulfillment:Fulfillment) {

  val sections = for(name <- sectionNames) yield new SectionReference(name)

  for(sectionRef <- sections) {
    if(fulfillment.hasSection(sectionRef.name)) {
      sectionRef.section = Some(fulfillment.getSectionByName(sectionRef.name))
    }
  }

  def promoteContingentReferences() = {
    var priorSectionRef:SectionReference = null

    for(sectionRef <- sections) {
      val currentSection = sectionRef.section.get
      priorSectionRef match {
        case sr: SectionReference =>
          if(sr.isValid) {
            sr.section.get.status match {
              case SectionStatus.TERMINAL =>
                if(currentSection.status == SectionStatus.CONTINGENT) {

                  // The prior section didn't complete successfully.. let's
                  // let the next section have a try
                  currentSection.setReady("Promoted from Contingent", DateTime.now)
                  currentSection.promoteContingentReferences(fulfillment) // <-- recurse
                }
              case _ => // We don't care about other status until a TERMINAL case is hit
            }
          }
          priorSectionRef.dismissed = true
        case _ =>
          // This is the first referenced section..
          if(currentSection.status == SectionStatus.CONTINGENT) {
            currentSection.setReady("Promoted from Contingent", DateTime.now)
            currentSection.promoteContingentReferences(fulfillment) // <-- recurse
          }
      }
      priorSectionRef = sectionRef
    }
  }

  def resolved():Boolean = {
    for(sectionRef <- sections) {
      sectionRef.section match {
        case section: Some[FulfillmentSection] =>
          section.get.status match {
            case SectionStatus.COMPLETE =>
              return true
            case _ =>
          }
        case _ =>
      }
    }
    false
  }

  def resolvable():Boolean = {
    for(sectionRef <- sections) {
      sectionRef.section match {
        case section: Some[FulfillmentSection] =>
          section.get.resolvable(fulfillment) match {
            case true =>
              return true
            case _ =>
          }
        case _ =>
      }
    }
    false
  }

  def getValue:JsValue = {
    for(sectionRef <- sections) {
      if(sectionRef.isValid && sectionRef.section.get.status == SectionStatus.COMPLETE) {
        try {
          return sectionRef.getValue
        } catch {
          case e: Exception =>

            val gripe = s"Referenced section ${sectionRef.section.get.name} is complete but the JSON could not be parsed! "+e.getMessage
            fulfillment.timeline.error(gripe, None)

            throw new Exception(gripe)
        }
      }
    }

    val gripe = "Tried to get value from referenced sections and no value was available! "+toString()
    fulfillment.timeline.error(gripe, None)

    throw new Exception(gripe)
  }

  override def toString: String = s"sections($sectionNames)"

  def toJson:JsValue = {
    Json.toJson(
      for(section <- sections) yield section.toJson
    )
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

  val components = mutable.MutableList[ReferencePathComponent]()
  private val pattern = new Regex("""([^\[\]/]+)|(\[\d+\])""")
  private val matches = (for(m <- pattern.findAllIn(path)) yield m).toList
  val sectionName = matches.head
  for(part <- matches.slice(1, matches.length)) {
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

  override def toString = {
    Json.stringify(toJson)
  }

  def toJson: JsValue = {
    Json.toJson(for(component <- components) yield component.toJson)
  }
}

class SectionReference(referenceString:String) {

  protected var _sectionName = "-undefined-"
  var dismissed:Boolean = false
  var section:Option[FulfillmentSection] = None
  var path:Option[ReferencePath] = None

  ReferencePath.isJsonPath(referenceString) match {
    case false => _sectionName = referenceString
    case _ =>
      path = Some(new ReferencePath(referenceString))
      _sectionName = path.get.sectionName
  }

  def getValue:JsValue = {
    if(section.isEmpty) { return null; }
    path.isEmpty match {
      case true => section.get.value
      case _ =>
        path.get.getValue(section.get.value)
    }
  }

  def isValid:Boolean = {
    section.isDefined
  }

  def name:String = {
    _sectionName
  }

  def toJson:JsValue = {
    Json.obj(
      "name" -> name,
      "dismissed" -> dismissed,
      "path" -> (path.isDefined match {
        case true => path.get.toJson
        case _ => new JsArray
      }),
      "resolved" -> (section.isDefined match {
        case true => section.get.status == SectionStatus.COMPLETE
        case _ => false
      }),
      "value" ->
        (try {
          getValue
        } catch {
          case e: Exception =>
            new JsString("--ERROR--")
        })
    )
  }
}

