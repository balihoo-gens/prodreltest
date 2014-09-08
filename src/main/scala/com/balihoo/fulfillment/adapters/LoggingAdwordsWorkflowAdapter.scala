package com.balihoo.fulfillment.adapters

/*
 * trait to bundle the mixins for fulfillmentworker with an adwordsadapter
 * this is mixed in by any worker that needs adwords functionality
 */
trait LoggingAdwordsWorkflowAdapter
  extends LoggingWorkflowAdapter
  with AdWordsAdapterComponent

/*
 * Implementation trait for the LoggingAdwordsWorkflowAdapter
 * adwords worker mixin trait
 */
trait LoggingAdwordsWorkflowAdapterImpl
  extends LoggingAdwordsWorkflowAdapter
  with LoggingWorkflowAdapterImpl {

  lazy private val _awa = new AdWordsAdapter(_cfg)
  def adWordsAdapter = _awa
}
