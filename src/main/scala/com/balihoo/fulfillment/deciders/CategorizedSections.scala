package com.balihoo.fulfillment.deciders

import org.joda.time.DateTime

import scala.collection.mutable
/**
 * Bin the sections by status. So we can make decisions
 * @param fulfillment SectionMap
 */
class CategorizedSections(fulfillment: Fulfillment) {
  val complete = mutable.MutableList[FulfillmentSection]()
  val inprogress = mutable.MutableList[FulfillmentSection]()
  val timedout = mutable.MutableList[FulfillmentSection]()
  val deferred = mutable.MutableList[FulfillmentSection]()
  val blocked = mutable.MutableList[FulfillmentSection]()
  val failed = mutable.MutableList[FulfillmentSection]()
  val canceled = mutable.MutableList[FulfillmentSection]()
  val contingent = mutable.MutableList[FulfillmentSection]()
  val terminal = mutable.MutableList[FulfillmentSection]()
  val ready = mutable.MutableList[FulfillmentSection]()
  val impossible = mutable.MutableList[FulfillmentSection]()

  var essentialComplete = 0
  var essentialTotal = 0

  categorize()

  def categorize() = {

    essentialComplete = 0
    essentialTotal = 0

    complete.clear()
    inprogress.clear()
    timedout.clear()
    deferred.clear()
    blocked.clear()
    failed.clear()
    canceled.clear()
    contingent.clear()
    terminal.clear()
    ready.clear()
    impossible.clear()

    for((name, section) <- fulfillment.nameToSection) {
      if(section.essential) { essentialTotal += 1 }
      section.status match {
        case SectionStatus.COMPLETE =>
          complete += section
          if(section.essential) { essentialComplete += 1 }
        case SectionStatus.SCHEDULED =>
          inprogress += section
        case SectionStatus.STARTED =>
          inprogress += section
        case SectionStatus.FAILED =>
          failed += section
        case SectionStatus.CANCELED =>
          canceled += section
        case SectionStatus.CONTINGENT =>
          contingent += section
        case SectionStatus.TIMED_OUT =>
          timedout += section
        case SectionStatus.DEFERRED =>
          deferred += section
        case SectionStatus.TERMINAL =>
          terminal += section
        case SectionStatus.BLOCKED =>
          categorizeReadySection(section)
        case SectionStatus.READY =>
          categorizeReadySection(section)
        case SectionStatus.IMPOSSIBLE =>
          impossible += section
        case _ => println(section.status + " is not handled here!")
      }
    }
  }

  /**
   * This is a special case.. incomplete sections are either 'READY' or 'BLOCKED'
   * We have to examine the params/prereqs to know if this section is runnable yet
   * @param section FulfillmentSection
   * @return
   */
  protected def categorizeReadySection(section: FulfillmentSection) = {

    var paramsReady: Boolean = true
    for((name, param) <- section.params) {
      param.evaluate(fulfillment)
      paramsReady &= param.isResolved
    }

    var prereqsReady: Boolean = true
    for(prereq: String <- section.prereqs) {
      val referencedSection: FulfillmentSection = fulfillment.getSectionByName(prereq)
      referencedSection.status match {
        case SectionStatus.COMPLETE =>
          // println("Section is complete")
        case _ =>
          // Anything other than complete is BLOCKING our progress
          section.timeline.warning(s"Waiting for prereq $prereq (${referencedSection.status})", None)
          prereqsReady = false
      }
    }

    if(!paramsReady) {
      if(section.resolvable(fulfillment)) {
        section.setBlocked("Not all parameters are resolved!", DateTime.now())
        blocked += section
      } else {
        section.setImpossible("Impossible because some parameters can never be resolved!", DateTime.now())
        impossible += section
      }
    } else if(!prereqsReady) {
      section.setBlocked("Not all prerequisites are complete!", DateTime.now())
      blocked += section
    } else {
      // Whoohoo! we're ready to run!
      ready += section
    }

  }

  def workComplete() : Boolean = {
    if(fulfillment.size == 0) { return false; }

    essentialTotal match {
      case 0 =>
        // No essential sections.. we just want everything complete or contingent
        fulfillment.size == (complete.length + contingent.length)
      case _ =>
        // If there are essential sections.. they MUST ALL be COMPLETE
        essentialComplete == essentialTotal
    }
  }

  def hasPendingSections: Boolean = {
    inprogress.length + deferred.length != 0
  }

  def checkPromoted: Boolean = {
    for(section <- contingent) {
      if(section.status == SectionStatus.READY) {
        return true
      }
    }

    false
  }

}
