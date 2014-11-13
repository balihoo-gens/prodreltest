package com.balihoo.fulfillment.deciders

import com.balihoo.fulfillment.util.UTCFormatter
import org.joda.time.{Seconds, DateTime}

import com.amazonaws.services.simpleworkflow.model._
import play.api.libs.json._

import scala.collection.mutable

object SectionStatus extends Enumeration {
  val READY = Value("READY")
  val BLOCKED = Value("BLOCKED")
  val SCHEDULED = Value("SCHEDULED")
  val STARTED = Value("STARTED")
  val FAILED = Value("FAILED")
  val TIMED_OUT = Value("TIMED_OUT")
  val CANCELED = Value("CANCELED")
  val TERMINAL = Value("TERMINAL") // Section has FAILED/CANCELED/TIMED OUT too many times!
  val COMPLETE = Value("COMPLETE")
  val CONTINGENT = Value("CONTINGENT") // Special case. We won't attempt to process this unless necessary
  val DEFERRED = Value("DEFERRED") // A Timer will activate this later
  val IMPOSSIBLE = Value("IMPOSSIBLE") // Section can never be completed

}
class ActionParams(var maxRetries:Int, var delaySeconds:Int) {
  def toJson: JsValue = {
    Json.obj(
      "maxRetries" -> maxRetries,
      "delaySeconds" -> delaySeconds
    )
  }
}

object TimelineEventType extends Enumeration {
  val NOTE = Value("NOTE")
  val WARNING = Value("WARNING")
  val ERROR = Value("ERROR")
  val SUCCESS = Value("SUCCESS")
}

class TimelineEvent(val eventType:TimelineEventType.Value, val message:String, val when:Option[DateTime]) {

  def toJson: JsValue = {
    Json.obj(
      "eventType" -> eventType.toString,
      "message" -> message,
      "when" -> (if(when.isDefined) UTCFormatter.format(when.get) else "--")
    )
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
  val params = collection.mutable.Map[String, SectionParameter]()
  val prereqs = mutable.MutableList[String]()
  val timeline = new Timeline
  var value: JsValue = JsNull

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
          try {
            status = SectionStatus.withName(v.as[String])
          } catch {
            case nsee:NoSuchElementException =>
              timeline.error(s"'${v.as[String]}' is an invalid status! Using IMPOSSIBLE by default.", Some(creationDate))
              status = SectionStatus.IMPOSSIBLE
          }

        case "essential" =>
          essential = v.as[Boolean]

        case "fixable" =>
          fixable = v.as[Boolean]
          
        case "waitUntil" =>
          waitUntil = Some(new DateTime(v.as[String]))

        case "value" =>
          value = v

        case _ =>
          // Add anything we don't recognize as a note in the timeline
          timeline.note(s"$key : $v", Some(creationDate))
      }
    }
  }

  def jsonInitAction(jaction: JsValue) = {
    jaction match {
      case jObj: JsObject =>
        action = Some(new ActivityType)
        action.get.setName(jObj.value("name").as[String])
        action.get.setVersion(jObj.value("version").as[String])
        handleActionParams(jObj)

      case _ =>
        timeline.error(s"Action '${Json.stringify(jaction)}' is of type '${jaction.getClass.toString}'. This is not a valid type.", Some(creationDate))
    }
  }

  def jsonInitParams(jparams: JsObject) = {
    for((jk:String, jv:JsValue) <- jparams.fields) {
      params(jk) = new SectionParameter(jv)
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
    value = JsString(result)
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

  def setBlocked(note:String, when:DateTime) = {
    status = SectionStatus.BLOCKED
    timeline.warning("Blocked: "+note, Some(when))
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

  def promoteContingentReferences(fulfillment:Fulfillment):Boolean = {
    if(status == SectionStatus.READY) {
      for((pname, param) <- params) {
        // attempting to evaluate the parameter will cause any needed promotions
        param.evaluate(fulfillment)
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

  def gatherParameters():Map[String,JsValue] = {

    val oparams = mutable.Map[String, JsValue]()

    for((name, param) <- params) {
      if(!param.isResolved) {
        throw new Exception(s"Unresolved parameter '$name'!")
      }
      oparams(name) = Json.toJson(param.getResult.get)
    }

    oparams.toMap
  }

  def evaluateParameters(fulfillment:Fulfillment) = {
    for((name, param) <- params) {
      param.evaluate(fulfillment)
    }
  }

  def resolvable(fulfillment:Fulfillment): Boolean = {
    if(List(SectionStatus.IMPOSSIBLE, SectionStatus.TERMINAL).contains(status)) {
      return false
    }

    for((pname, param) <- params) {
      if(!param.isResolvable) {
        return false
      }
    }
    true
  }

  override def toString = {
    Json.stringify(toJson)
  }

  def toJson: JsValue = {
    val jparams = collection.mutable.Map[String, JsValue]()
    for((pname, param) <- params) {
      jparams(pname) = param.toJson
    }

    val jtimeline = Json.toJson(for(entry <- timeline.events) yield entry.toJson)

    Json.obj(
      "status" -> status.toString,
      "timeline" -> jtimeline,
      "value" -> value,
      "input" -> jsonNode,
      "params" -> Json.toJson(jparams.toMap),
      "essential" -> essential,
      "fixable" -> fixable,
      "scheduledCount" -> scheduledCount,
      "startedCount" -> startedCount,
      "timedoutCount" -> timedoutCount,
      "canceledCount" -> canceledCount,
      "failedCount" -> failedCount,
      "failureParams" -> failureParams.toJson,
      "timeoutParams" -> timeoutParams.toJson,
      "cancelationParams" -> cancelationParams.toJson,
      "waitUntil" ->
        (waitUntil.isDefined match {
          case true =>
            UTCFormatter.format(waitUntil.get)
          case _ => ""
        })
    )
  }
}

class ReferenceNotReady(message:String) extends Exception(message)
class ReferenceNotResolvable(message:String) extends Exception(message)

class SectionParameter(input:JsValue) {

  protected var record:JsValue = JsNull
  protected var fulfillment:Option[Fulfillment] = None
  protected val inputString = Json.stringify(input)
  protected val needsEvaluation = inputString contains "<("

  protected var evaluated:Boolean = !needsEvaluation
  protected var result:Option[JsValue] = evaluated match {
    case true => Some(input)
    case _ => None

  }
  protected var resolvable:Boolean = true

  protected def _evaluateJsValue(js:JsValue):JsValue = {
    js match {
      case jObj: JsObject =>
        _evaluateJsObject(jObj)
      case jArr: JsArray =>
        _evaluateJsArray(jArr)
      case _ =>
        js
    }
  }

  protected def _evaluateJsObject(jObj:JsObject):JsValue = {
    jObj.fields.size match {
      case 1 =>
        val firstKey = jObj.keys.head.toLowerCase
        if(firstKey.startsWith("<(section)>")) { // SECTIONS are special..
          return _processReference(
            jObj.value("<(section)>") match {
              case ss:JsString => List(ss.value)
              case ls:JsArray => ls.as[List[String]]
              case _ =>
                throw new Exception("Section References must be a String or Array[String]")
            })
        } else if(firstKey.startsWith("<(")) {
          return _processOperator(jObj)
        }
      case _ =>
    }

    // This Object isn't an operator.. just recurse and return
    Json.toJson((for((k, v) <- jObj.fields) yield k -> _evaluateJsValue(v)).toMap)
  }

  protected def _evaluateJsArray(jArr:JsArray):JsValue = {
    Json.toJson(for(j <- jArr.value) yield _evaluateJsValue(j))
  }

  protected def _processOperator(jObj:JsObject):JsValue = {
    val (rawop, operand) : (String, JsValue) = jObj.fields(0)
    val opName = rawop.substring(2, rawop.length - 2) // trim off the <( .. )>

    JsonOps(JsonOpName.withName(opName.toLowerCase), _evaluateJsValue(operand))
  }

  protected def _processReference(nameList:List[String]):JsValue = {
    val sectionRef = new SectionReferences(nameList, fulfillment.get)
    sectionRef.resolved() match {
      case true => sectionRef.getValue
      case false =>
        resolvable &= sectionRef.resolvable()
        resolvable match {
          case true =>
            sectionRef.promoteContingentReferences()
            throw new ReferenceNotReady(sectionRef.toString)
          case false =>
            throw new ReferenceNotResolvable(sectionRef.toString)
        }
    }
  }

  def evaluate(f:Fulfillment) = {
    if(!evaluated) {
      evaluated = true
      fulfillment = Some(f)
      try {
        result = Some(_evaluateJsValue(input))
      } catch {
        case rne:ReferenceNotReady =>
          f.timeline.note(s"Reference not ready! ${rne.getMessage}", Some(DateTime.now()))
        case rnr:ReferenceNotResolvable =>
          f.timeline.warning(s"Reference not resolvable! ${rnr.getMessage}", Some(DateTime.now()))
        case e: Exception => // Unexpected! Throw!
          throw e

      }
    }
  }

  def getResult:Option[JsValue] = {
    result
  }

  def isResolved:Boolean = {
    result.isDefined
  }

  def isResolvable:Boolean = {
    resolvable
  }

  def isEvaluated:Boolean = {
    evaluated
  }

  def toJson:JsValue = {
    Json.obj(
      "input" -> input,
      "result" -> (if(result.isDefined) { result.get } else { JsNull }),
      "resolvable" -> resolvable,
      "resolved" -> result.isDefined,
      "evaluated" -> evaluated,
      "needsEvaluation" -> needsEvaluation
    )
  }
}
