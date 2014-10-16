package com.balihoo.fulfillment.adapters
import com.balihoo.fulfillment.util.{SploggerComponent, Splogger}
import com.balihoo.fulfillment.config.PropertiesLoader

/*
 * trait to bundle the mixins for fulfillmentworker
 * this is mixed in by any worker
 */
trait LoggingWorkflowAdapter
  extends AnyRef
    with SWFAdapterComponent
    with DynamoAdapterComponent
    with SploggerComponent

/*
 * Implementation trait for the LoggingWorkflowAdapter
 * defining the implementations of the mixins
 */
trait LoggingWorkflowAdapterImpl
  extends LoggingWorkflowAdapter {
  val _cfg: PropertiesLoader
  val _splog: Splogger

  def splog = _splog

  lazy private val _swf = new SWFAdapter(_cfg, _splog, true)
  def swfAdapter = _swf

  lazy private val _dyn = new DynamoAdapter(_cfg)
  def dynamoAdapter = _dyn
}


