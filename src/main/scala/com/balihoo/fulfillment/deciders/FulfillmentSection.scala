package com.balihoo.fulfillment.deciders

import java.security.MessageDigest

import com.balihoo.fulfillment.workers.UTCFormatter
import org.joda.time.{Seconds, DateTime}

import com.amazonaws.services.simpleworkflow.model._
import play.api.libs.json._

import scala.collection.mutable
import scala.util.matching.Regex

object SectionStatus extends Enumeration {
  val READY = Value("READY")
  val SCHEDULED = Value("SCHEDULED")
  val STARTED = Value("STARTED")
  val FAILED = Value("FAILED")
  val TIMED_OUT = Value("TIMED OUT")
  val CANCELED = Value("CANCELED")
  val TERMINAL = Value("TERMINAL") // Section has FAILED/CANCELED/TIMED OUT too many times!
  val COMPLETE = Value("COMPLETE")
  val CONTINGENT = Value("CONTINGENT") // Special case. We won't attempt to process this unless necessary
  val DEFERRED = Value("DEFERRED") // A Timer will activate this later
  val IMPOSSIBLE = Value("IMPOSSIBLE") // Section can never be completed

}

class ActionParams(var maxRetries:Int, var delaySeconds:Int) {
}

object TimelineEventType extends Enumeration {
  val NOTE = Value("NOTE")
  val WARNING = Value("WARNING")
  val ERROR = Value("ERROR")
  val SUCCESS = Value("SUCCESS")
}

class TimelineEvent(val eventType:TimelineEventType.Value, val message:String, val when:Option[DateTime]) {

  def toJson: JsValue = {
    Json.toJson(Map(
      "eventType" -> Json.toJson(eventType.toString),
      "message" -> Json.toJson(message),
      "when" -> Json.toJson(if(when.isDefined) UTCFormatter.format(when.get) else "--")
    ))
  }
}

class Timeline {
  val events = mutable.MutableList[TimelineEvent]()

  def error(message:String, when:Option[DateTime]) = {
    events += new TimelineEvent(TimelineEventType.ERROR, message, when)
  }

  def warning(message:String, when:Option[DateTime]) = {
    events += new TimelineEvent(TimelineEventType.WARNING, message, when)
  }

  def note(message:String, when:Option[DateTime]) = {
    events += new TimelineEvent(TimelineEventType.NOTE, message, when)
  }

  def success(message:String, when:Option[DateTime]) = {
    events += new TimelineEvent(TimelineEventType.SUCCESS, message, when)
  }
}

class FulfillmentSection(val name: String
                         ,val jsonNode: JsObject
                         ,val creationDate:DateTime) {

  var action: Option[ActivityType] = None
  var operator: Option[String] = None
  val params = collection.mutable.Map[String, Any]()
  val prereqs = mutable.MutableList[String]()
  val timeline = new Timeline
  var value: String = ""

  var status = SectionStatus.CONTINGENT

  var essential = false
  var fixable = true

  var scheduledCount: Int = 0
  var startedCount: Int = 0
  var timedoutCount: Int = 0
  var canceledCount: Int = 0
  var failedCount: Int = 0

  // 3 events of each type. 10 minute wait.
  val failureParams = new ActionParams(3, 600)
  val timeoutParams = new ActionParams(3, 600)
  val cancelationParams = new ActionParams(3, 600)

  var startToCloseTimeout: Option[String] = None
  var scheduleToStartTimeout: Option[String] = None
  var scheduleToCloseTimeout: Option[String] = None
  var heartbeatTimeout: Option[String] = None
  var waitUntil: Option[DateTime] = None
  
  jsonInit(jsonNode)

  def jsonInit(jsonNode: JsObject) = {
    for((key, v) <- jsonNode.fields) {
      key match {
        case "action" =>
          jsonInitAction(v)

        case "params" =>
          jsonInitParams(v.as[JsObject])

        case "prereqs" =>
          jsonInitPrereqs(v.as[JsArray])

        case "status" =>
          status = SectionStatus.withName(v.as[String])

        case "essential" =>
          essential = v.as[Boolean]

        case "fixable" =>
          fixable = v.as[Boolean]
          
        case "waitUntil" =>
          waitUntil = Some(new DateTime(v.as[String]))

        case "value" =>
          value = v.as[String]

        case _ =>
          // Add anything we don't recognize as a note in the timeline
          timeline.note(s"$key : $v", Some(creationDate))
      }
    }
  }

  def jsonInitAction(jaction: JsValue) = {
    jaction match {
      case jObj:JsObject =>
        action = Some(new ActivityType)
        action.get.setName(jObj.value("name").as[String])
        action.get.setVersion(jObj.value("version").as[String])
        handleActionParams(jObj)
      case jStr:JsString =>
        operator = Some(jStr.as[String])
      case _ =>
        timeline.error(s"Action '${Json.stringify(jaction)}' is of type '${jaction.getClass.toString}'. This is not a valid type.", Some(creationDate))
    }
  }

  def jsonInitParams(jparams: JsObject) = {
    for((jk, jv) <- jparams.fields) {
      jv match {
        case jArr: JsArray =>
          params(jk) = new SectionReferences(jArr.as[List[String]])
        case jStr: JsString =>
          params(jk) = jv.as[String]
        case _ =>
          timeline.error(s"Parameter '$jk' is of type '${jv.getClass.toString}'. This is not a valid type.", Some(creationDate))
      }
    }
  }

  def jsonInitPrereqs(jprereqs:JsArray) = {
    prereqs.clear()
    prereqs ++= jprereqs.as[List[String]]
  }

  def handleActionParams(jaction:JsObject) = {
    for((akey, avalue) <- jaction.fields) {
      akey match {
        case "failure" =>
          val failure = avalue.as[JsObject]
          failureParams.maxRetries =  failure.value("max").as[String].toInt
          failureParams.delaySeconds =  failure.value("delay").as[String].toInt
        case "timeout" =>
          val timeout = avalue.as[JsObject]
          timeoutParams.maxRetries =  timeout.value("max").as[String].toInt
          timeoutParams.delaySeconds =  timeout.value("delay").as[String].toInt
        case "cancelation" =>
          val cancelation = avalue.as[JsObject]
          cancelationParams.maxRetries =  cancelation.value("max").as[String].toInt
          cancelationParams.delaySeconds =  cancelation.value("delay").as[String].toInt
        case "startToCloseTimeout" =>
          startToCloseTimeout = Some(avalue.as[String])
        case "scheduleToCloseTimeout" =>
          scheduleToCloseTimeout = Some(avalue.as[String])
        case "scheduleToStartTimeout" =>
          scheduleToStartTimeout = Some(avalue.as[String])
        case "heartbeatTimeout" =>
          heartbeatTimeout = Some(avalue.as[String])
        case _ =>
      }
    }
  }

  def setReady(reason:String, when:DateTime) = {
    if(status == SectionStatus.TERMINAL) {
      timeline.error("Can't set status to READY because section is TERMINAL!", Some(when))
    } else {
      status = SectionStatus.READY
      timeline.note("READY: "+reason, Some(when))
    }
  }

  def setStarted(when:DateTime) = {
    startedCount += 1
    status = SectionStatus.STARTED
    timeline.note("Started", Some(when))
  }

  def setScheduled(when:DateTime) = {
    scheduledCount += 1
    status = SectionStatus.SCHEDULED
    timeline.note("Scheduled", Some(when))
  }

  def setCompleted(result:String, when:DateTime) = {
    status = SectionStatus.COMPLETE
    timeline.success("Completed", Some(when))
    value = result
  }

  def setFailed(reason:String, details:String, when:DateTime) = {
    failedCount += 1
    timeline.warning(s"Failed because:$reason $details", Some(when))
    status = SectionStatus.FAILED
    if(failedCount > failureParams.maxRetries) {
      setTerminal(s"Failed too many times! ($failedCount > ${failureParams.maxRetries})", when)
    }
  }

  def setCanceled(details:String, when:DateTime) = {
    canceledCount += 1
    timeline.warning(s"Canceled because: $details", Some(when))
    status = SectionStatus.CANCELED
    if(canceledCount > cancelationParams.maxRetries) {
      setTerminal(s"Canceled too many times! ($canceledCount > ${cancelationParams.maxRetries})", when)
    }
  }

  def setTimedOut(tot:String, details:String, when:DateTime) = {
    timedoutCount += 1
    timeline.warning("Timed out! "+tot+" "+details, Some(when))
    status = SectionStatus.TIMED_OUT
    if(timedoutCount > timeoutParams.maxRetries) {
      setTerminal(s"Timed out too many times! ($timedoutCount > ${timeoutParams.maxRetries})", when)
    }
  }

  def setContingent(reason:String, when:DateTime) = {
    status = SectionStatus.CONTINGENT
    timeline.note("Contingent: "+reason, Some(when))
  }

  def setImpossible(reason:String, when:DateTime) = {
    status = SectionStatus.IMPOSSIBLE
    timeline.error(reason, Some(when))
  }

  def setTerminal(reason:String, when:DateTime) = {
    status = SectionStatus.TERMINAL
    timeline.error(reason, Some(when))
  }

  def setDeferred(note:String, when:DateTime) = {
    status = SectionStatus.DEFERRED
    timeline.note("Deferred: "+note, Some(when))
  }

  def setStatus(ss:String, message:String, when:DateTime) = {
    SectionStatus.withName(ss) match {
      case SectionStatus.READY =>
        setReady(message, when)
      case SectionStatus.FAILED =>
        setFailed("FAILED", message, when)
      case SectionStatus.TERMINAL =>
        setTerminal(message, when)
      case _ =>
        throw new Exception(s"Can't generically set status to '$ss'!")
    }
  }

  def resolveReferences(map:FulfillmentSections):Boolean = {
    if(status == SectionStatus.READY) {
      for((pname, param) <- params) {
        param match {
          case sectionReferences: SectionReferences =>
            sectionReferences.processReferences(map)
          case _ =>
        }
      }
    }
    true // <-- cause it's recursive... I guess..
  }

  def getActivityId = {
    val timestamp: Long = System.currentTimeMillis()
    s"$name${Constants.delimiter}${action.get.getName}${Constants.delimiter}"+timestamp
  }

  /**
   * Calculates the proper wait time for a task.  If there is no waitUntil value, the wait time will be zero.
   * @return the wait time in seconds
   */
  def calculateWaitSeconds(): Int = {
    waitUntil match {
      case Some(d) => Seconds.secondsBetween(DateTime.now, d).getSeconds
      case _ => 0
    }
  }

  override def toString = {
    Json.stringify(toJson)
  }

  def toJson: JsValue = {
    val jparams = collection.mutable.Map[String, JsValue]()
    for((pname, param) <- params) {
      param match {
        case sectionReferences: SectionReferences =>
          jparams(pname) = sectionReferences.toJson
        case s:String =>
          jparams(pname) = Json.toJson(s)
        case _ =>
      }
    }

    val jtimeline = Json.toJson(for(entry <- timeline.events) yield entry.toJson)

    Json.toJson(Map(
      "status" -> Json.toJson(status.toString),
      "timeline" -> Json.toJson(jtimeline),
      "value" -> Json.toJson(value),
      "input" -> Json.toJson(jsonNode),
      "params" -> Json.toJson(jparams.toMap),
      "essential" -> Json.toJson(essential),
      "fixable" -> Json.toJson(fixable),
      "scheduledCount" -> Json.toJson(scheduledCount),
      "startedCount" -> Json.toJson(startedCount),
      "timedoutCount" -> Json.toJson(timedoutCount),
      "canceledCount" -> Json.toJson(canceledCount),
      "failedCount" -> Json.toJson(failedCount),
      "waitUntil" ->
        Json.toJson(waitUntil.isDefined match {
          case true =>
            UTCFormatter.format(waitUntil.get)
          case _ => ""
        })
    ))
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

  def getValue(jsonString:String):String = {
    var current:JsValue = Json.parse(jsonString)
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

    current.as[String]
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

  def getValue:String = {
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
    Json.toJson(Map(
      "name" -> Json.toJson(name),
      "dismissed" -> Json.toJson(dismissed),
      "path" -> (path.isDefined match {
        case true => path.get.toJson
        case _ => new JsArray
      }),
      "resolved" -> (section.isDefined match {
        case true => Json.toJson(section.get.status == SectionStatus.COMPLETE)
        case _ => Json.toJson(false)
      }),
      "value" -> Json.toJson(getValue)
    ))
  }
}

class SectionReferences(sectionNames:List[String]) {

  val sections = for(name <- sectionNames) yield new SectionReference(name)

  def hydrate(map:FulfillmentSections) = {
    for(sectionRef <- sections) {
      if(map.hasSection(sectionRef.name)) {
        sectionRef.section = Some(map.getSectionByName(sectionRef.name))
      }
    }
  }

  def processReferences(map:FulfillmentSections) = {
    hydrate(map)

    var priorSectionRef:SectionReference = null

    for(sectionRef <- sections) {
      priorSectionRef match {
        case sr: SectionReference =>
          if(sr.isValid) {
            sr.section.get.status match {
              case SectionStatus.TERMINAL =>
                if(sectionRef.section.get.status == SectionStatus.CONTINGENT) {

                  // The prior section didn't complete successfully.. let's
                  // let the next section have a try
                  sectionRef.section.get.setReady("Promoted from Contingent", DateTime.now)
                  sectionRef.section.get.resolveReferences(map) // <-- recurse
                }
              case _ => // We don't care about other status until a TERMINAL case is hit
            }
          }
          priorSectionRef.dismissed = true
        case _ =>
          // This is the first referenced section..
      }
      priorSectionRef = sectionRef
    }
  }

  def resolved(map:FulfillmentSections):Boolean = {
    hydrate(map)

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

  def getValue(map:FulfillmentSections):String = {
    hydrate(map)

    for(sectionRef <- sections) {
      if(sectionRef.isValid && sectionRef.section.get.status == SectionStatus.COMPLETE) {
        return sectionRef.getValue
      }
    }

    val gripe = "Tried to get value from referenced sections and no value was available! "+toString()
    map.timeline.error(gripe, None)

    throw new Exception(gripe)
  }

  override def toString: String = s"sections($sectionNames)"

  def toJson:JsValue = {
    Json.toJson(
      for(section <- sections) yield section.toJson
    )
  }
}

