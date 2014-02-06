package com.balihoo.fulfillment

import com.balihoo.fulfillment.workers.PrototypeListProviderWorkerConfig

object WorkflowRegister {
  def main(args: Array[String]) {
    PrototypeWorkflowExecutorConfig.registerWorkflowType()
    PrototypeListProviderWorkerConfig.registerActivityType()
    /*
    Note: It is possible to register domain through this API as well, however this should
    probably be a pretty major task and we won't want to register new ones all the time.
    For now, this can be only done manually through the AWS site, but if we change our minds
    about that this is where we could client.registerDomain
     */
  }
}