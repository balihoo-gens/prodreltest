package com.balihoo.fulfillment.deciders

import com.balihoo.fulfillment.util.UTCFormatter
import com.fasterxml.jackson.core.JsonParseException
import org.joda.time.{Seconds, DateTime}

import com.amazonaws.services.simpleworkflow.model._
import play.api.libs.json._

import scala.collection.mutable

object SectionStatus extends Enumeration {
  val BLOCKED = Value("BLOCKED")
  val CANCELED = Value("CANCELED")
  val COMPLETE = Value("COMPLETE")
  val CONTINGENT = Value("CONTINGENT") // Special case. We won't attempt to process this unless necessary
  val DEFERRED = Value("DEFERRED") // A Timer will activate this later
  val FAILED = Value("FAILED")
  val IMPOSSIBLE = Value("IMPOSSIBLE") // Section can never be completed
  val READY = Value("READY")
  val SCHEDULED = Value("SCHEDULED")
  val STARTED = Value("STARTED")
  val TERMINAL = Value("TERMINAL") // Section has FAILED/CANCELED/TIMED OUT too many times!
  val TIMED_OUT = Value("TIMED_OUT")
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

class TimelineEvent(val eventType:TimelineEventType.Value, val message:String, val when:Option[DateTime] = None) {

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

  def error(message:String, when:Option[DateTime] = None) = {
    events += new TimelineEvent(TimelineEventType.ERROR, message, when)
  }

  def warning(message:String, when:Option[DateTime] = None) = {
    events += new TimelineEvent(TimelineEventType.WARNING, message, when)
  }

  def note(message:String, when:Option[DateTime] = None) = {
    events += new TimelineEvent(TimelineEventType.NOTE, message, when)
  }

  def success(message:String, when:Option[DateTime] = None) = {
    events += new TimelineEvent(TimelineEventType.SUCCESS, message, when)
  }

  def toJson: JsValue = Json.toJson(for(entry <- events) yield entry.toJson)
}

class FulfillmentSection(val name: String
                         ,val jsonNode: JsObject
                         ,val creationDate:DateTime) {

  var action: Option[ActivityType] = None
  val params = collection.mutable.Map[String, SectionParameter]()
  val prereqs = mutable.MutableList[String]()
  val subsections = mutable.Map[String, FulfillmentSection]()
  var parent: Option[FulfillmentSection] = None
  val evaluationContext = collection.mutable.Map[String, JsValue]()
  val timeline = new Timeline
  private var _value: JsValue = JsNull
  def value = _value

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

  var multiParamName:Option[String] = None

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
          _value = v

        case "multiParam" =>
          multiParamName = Some(v.as[String])

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
    try {
      // We expect results to come back as legal JSON...
      _value = Json.parse(result)
    } catch {
      case jpe:JsonParseException =>
        // Wasn't json encoded, it's automatically a JSON string..
        _value = JsString(result)
    }
    if(parent.isDefined) {
      parent.get.resolveMultiSection()
    }
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
    if(parent.isDefined) {
      parent.get.resolveMultiSection()
    }
  }

  def setTerminal(reason:String, when:DateTime) = {
    status = SectionStatus.TERMINAL
    timeline.error(reason, Some(when))
    if(parent.isDefined) {
      parent.get.resolveMultiSection()
    }
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

  def promoteContingentReferences(fulfillment:Fulfillment) = {
    if(status == SectionStatus.READY) {
      // attempting to evaluate the parameters will cause any needed promotions
      evaluateParameters(fulfillment)
    }
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
      oparams(name) = param.getResult.get
    }

    oparams.toMap
  }

  def evaluateParameters(fulfillment:Fulfillment) = {
    if(multiParamName.isDefined) {
        createSubsections(fulfillment)
    } else {
      for((name, param) <- params) {
        param.evaluate(fulfillment, evaluationContext.toMap)
      }
    }
  }

  def paramsResolved():Boolean = {
    params.forall(_._2.isResolved)
  }

  def resolved:Boolean = {
    List(SectionStatus.IMPOSSIBLE, SectionStatus.TERMINAL, SectionStatus.COMPLETE).contains(status) 
  }

  def subsectionsResolved:Boolean = {
    subsections.forall(_._2.resolved)
  }

  def resolveMultiSection() = {
    if(!multiParamName.isDefined) {
      throw new Exception("Section is not multi-param! Can't resolve it as such!")
    }
    if(subsectionsResolved) {
      status = SectionStatus.COMPLETE
      val multiParam = params(multiParamName.get)
      multiParam.getResult.get match {
        case arr:JsArray =>
          val lresults = mutable.MutableList[JsValue]()
          for((p, index) <- arr.value.zipWithIndex) {
            val subSectionName = s"$name[$index]"
            lresults += subsections(subSectionName).value
          }
          _value = Json.arr(lresults.toList)
        case obj:JsObject =>
          val mresults = collection.mutable.Map[String, JsValue]()
          for((key, value) <- obj.fields) {
            val subSectionName = s"$name[$key]"
            mresults(key) = subsections(subSectionName).value
          }
          _value = Json.toJson(mresults.toMap)
        case _ =>
          throw new Exception("Expected an Array or Object for a multi-param result!")
      }
    } else {
      timeline.note("Not all subsections are resolved!")
    }
  }

  def isResolvable: Boolean = {
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

  def prereqsReady(fulfillment:Fulfillment):Boolean = {
    for(prereq: String <- prereqs) {
      val referencedSection: FulfillmentSection = fulfillment.getSectionByName(prereq)
      referencedSection.status match {
        case SectionStatus.COMPLETE =>
        // println("Section is complete")
        case _ =>
          // Anything other than complete is BLOCKING our progress
          timeline.warning(s"Waiting for prereq $prereq (${referencedSection.status})", None)
          return false
      }
    }
    true
  }

  def createSubsections(fulfillment:Fulfillment) = {

    val multiParam = params(multiParamName.get)
    multiParam.evaluate(fulfillment, evaluationContext.toMap)
    if(multiParam.isResolved && subsections.isEmpty) {

      multiParam.getResult.get match {
        case arr: JsArray =>
          status = SectionStatus.STARTED
          for((p, index) <- arr.value.zipWithIndex) {
            val newSectionName = s"$name[$index]"
            if(!subsections.contains(newSectionName)) {
              val newSection = new FulfillmentSection(newSectionName, jsonNode - "multiParam", DateTime.now())
              newSection.evaluationContext("multi-index") = JsNumber(index)
              newSection.evaluationContext("multi-value") = p
              // overwrite the multi-param with the resolved value
              newSection.params(multiParamName.get) = new SectionParameter(p)
              newSection.parent = Some(this)
              // TODO maybe a fulfillment.addSection() makes more sense..
              fulfillment.nameToSection(newSectionName) = newSection
              fulfillment.timeline.note(s"Adding new section $newSectionName")
              subsections(newSectionName) = newSection
            }
          }
        case obj: JsObject =>
          status = SectionStatus.STARTED
          for((key, value) <- obj.fields) {
            val newSectionName = s"$name[$key]"
            if(!subsections.contains(newSectionName)) {
              val newSection = new FulfillmentSection(newSectionName, jsonNode - "multiParam", DateTime.now())
              newSection.evaluationContext("multi-key") = JsString(key)
              newSection.evaluationContext("multi-value") = value
              // overwrite the multi-param with the resolved value
              newSection.params(multiParamName.get) = new SectionParameter(value)
              newSection.parent = Some(this)
              // TODO maybe a fulfillment.addSection() makes more sense..
              fulfillment.nameToSection(newSectionName) = newSection
              fulfillment.timeline.note(s"Adding new section $newSectionName")
              subsections(newSectionName) = newSection
            }
          }
        case _ =>
          throw new Exception("Expected an Array or Object for a multi-param result!")

      }
    }
  }
  
  def refineReadyStatus(fulfillment:Fulfillment, when:DateTime = DateTime.now()) = {

    if(List(SectionStatus.BLOCKED, SectionStatus.READY).contains(status)) {
      evaluateParameters(fulfillment)

      if(!paramsResolved()) {
        if(isResolvable) {
          setBlocked("Not all parameters are resolved!", when)
        } else {
          setImpossible("Impossible because some parameters can never be resolved!", when)
        }
        } else if(!prereqsReady(fulfillment)) {
          setBlocked("Not all prerequisites are complete!", when)
        } else {
          // Whoohoo! we're ready to run!
          setReady("All parameters and prereqs are resolved!", when)
        }
    }
  }

  override def toString = {
    Json.stringify(toJson)
  }

  def toJson: JsValue = {
    val jparams = collection.mutable.Map[String, JsValue]()
    for((pname, param) <- params) {
      jparams(pname) = param.toJson
    }

    Json.obj(
      "status" -> status.toString,
      "timeline" -> timeline.toJson,
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
      "subsections" -> subsections.keys,
      "parent" -> (if(parent.isDefined) parent.get.name else JsNull),
      "evaluationContext" -> Json.toJson(evaluationContext.toMap),
      "waitUntil" ->
        (waitUntil.isDefined match {
          case true =>
            UTCFormatter.format(waitUntil.get)
          case _ => ""
        })
    )
  }
}

class ReferenceNotResolved(message:String) extends Exception(message)
class ReferenceNotResolvable(message:String) extends Exception(message)

class SectionParameter(input:JsValue) {

  protected val timeline = new Timeline
  protected var record:JsValue = JsNull
  protected var fulfillment:Option[Fulfillment] = None
  protected val evaluationContext = mutable.Map[String, JsValue]()
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
        } else if(firstKey.startsWith("<(context)>")) {
          return _processContext(jObj.value("<(context)>").as[String])
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

    val operator = try {
      JsonOpName.withName(opName.toLowerCase)
    } catch {
      case nsee:NoSuchElementException =>
        throw new Exception(s"There is no operator '$rawop' available!")
    }

    JsonOps(operator, _evaluateJsValue(operand))
  }

  protected def _processContext(key:String):JsValue = {
    evaluationContext.getOrElse(key, throw new Exception(s"'$key' not available!"))
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
            throw new ReferenceNotResolved(sectionRef.toString)
          case false =>
            throw new ReferenceNotResolvable(sectionRef.toString)
        }
    }
  }

  def evaluate(f:Fulfillment, context:Map[String, JsValue]) = {
    if(!evaluated) {
      evaluated = true // Dont' confuse with 'resolved'! This just means we touched it.
      fulfillment = Some(f)
      evaluationContext.clear()
      evaluationContext ++= context
      try {
        result = Some(_evaluateJsValue(input))
      } catch {
        // We don't return or rethrow from any of these. We want to keep processing!
        case rne:ReferenceNotResolved =>
          timeline.warning(s"Reference not resolved! ${rne.getMessage}", Some(DateTime.now()))
        case rnr:ReferenceNotResolvable =>
          timeline.error(s"Reference not resolvable! ${rnr.getMessage}", Some(DateTime.now()))
          resolvable = false
        case e:Exception =>
          timeline.error("Unexpected Exception! "+ e.getMessage, Some(DateTime.now()))
          resolvable = false
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
      "needsEvaluation" -> needsEvaluation,
      "timeline" -> timeline.toJson
    )
  }
}
