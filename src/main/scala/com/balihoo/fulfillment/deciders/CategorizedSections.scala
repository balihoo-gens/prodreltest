package com.balihoo.fulfillment.deciders

import scala.collection.mutable
/**
 * Bin the sections by status. So we can make decisions
 * @param sections SectionMap
 */
class CategorizedSections(sections: SectionMap) {
  val complete = mutable.MutableList[FulfillmentSection]()
  val inprogress = mutable.MutableList[FulfillmentSection]()
  val timedout = mutable.MutableList[FulfillmentSection]()
  val deferred = mutable.MutableList[FulfillmentSection]()
  val blocked = mutable.MutableList[FulfillmentSection]()
  val failed = mutable.MutableList[FulfillmentSection]()
  val canceled = mutable.MutableList[FulfillmentSection]()
  val contingent = mutable.MutableList[FulfillmentSection]()
  val dismissed = mutable.MutableList[FulfillmentSection]()
  val terminal = mutable.MutableList[FulfillmentSection]()
  val ready = mutable.MutableList[FulfillmentSection]()
  val impossible = mutable.MutableList[FulfillmentSection]()

  for((name, section) <- sections.map) {
    section.status match {
      case SectionStatus.COMPLETE =>
        complete += section
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
      case SectionStatus.DISMISSED =>
        dismissed += section
      case SectionStatus.INCOMPLETE =>
        categorizeIncompleteSection(section)
      case SectionStatus.IMPOSSIBLE =>
        impossible += section
      case _ => println(section.status + " is not handled here!")
    }
  }

  /**
   * This is a special case.. incomplete sections are either 'READY' or 'BLOCKED'
   * We have to examine the params/prereqs to know if this section is runnable yet
   * @param section FulfillmentSection
   * @return
   */
  protected def categorizeIncompleteSection(section: FulfillmentSection) = {

    var paramsReady: Boolean = true
    for((name, value) <- section.params) {
      value match {
        case sectionReference: SectionReference =>
          paramsReady &= sectionReference.resolved(sections)
        case _ =>
          // non-reference params are automatically ready..
      }
    }

    var prereqsReady: Boolean = true
    for(prereq: String <- section.prereqs) {
      val referencedSection: FulfillmentSection = sections.map(prereq)
      referencedSection.status match {
        case SectionStatus.COMPLETE =>
          // println("Section is complete")
        case _ =>
          // Anything other than complete is BLOCKING our progress
          section.notes += s"Waiting for prereq $prereq (${referencedSection.status})"
          prereqsReady = false
      }
    }

    if(!paramsReady || !prereqsReady) {
      blocked += section
    } else {
      // Whoohoo! we're ready to run!
      ready += section
    }

  }

  def workComplete() : Boolean = {
    sections.map.size == (complete.length + contingent.length + dismissed.length)
  }

  def hasPendingSections: Boolean = {
    inprogress.length + deferred.length != 0
  }

}
