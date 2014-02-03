package com.balihoo.fulfillment

import com.amazonaws.services.simpleworkflow.model._
import com.balihoo.fulfillment.config.WorkflowConfig
import com.balihoo.fulfillment.deciders.PrototypeDeciderConfig

//todo: define a base that defines required WF params.
object PrototypeWorkflowExecutorConfig {
  val workflowType: WorkflowType = new WorkflowType()
  workflowType.setName("PrototypeWorkflowType")
  workflowType.setVersion("0.1")

  val workflowTypeDescription: String = "Prototype fulfillment workflow"

  val workflowStartToClose: Int = 60
  val deciderStartToClose: Int = 5

  def registerWorkflowType() {

    println("checking if workflow type " + workflowType.getName + ", version " + workflowType.getVersion + " exists")

    try{
      val describeWorkflowType: DescribeWorkflowTypeRequest = new DescribeWorkflowTypeRequest
      describeWorkflowType.setWorkflowType(workflowType)
      describeWorkflowType.setDomain(WorkflowConfig.domain)
      WorkflowConfig.client.describeWorkflowType(describeWorkflowType)
      println("workflow type already exists")
    } catch {
      case e: com.amazonaws.services.simpleworkflow.model.UnknownResourceException => {
        println("workflow type doesn't exist, registering")

        val registerRequest: RegisterWorkflowTypeRequest = new RegisterWorkflowTypeRequest
        registerRequest.setDomain(WorkflowConfig.domain)
        registerRequest.setName(workflowType.getName)
        registerRequest.setVersion(workflowType.getVersion)
        registerRequest.setDescription(workflowTypeDescription)
        registerRequest.setDefaultTaskList(PrototypeDeciderConfig.taskList)
        registerRequest.setDefaultTaskStartToCloseTimeout(deciderStartToClose.toString)
        registerRequest.setDefaultExecutionStartToCloseTimeout(workflowStartToClose.toString)
        registerRequest.setDefaultChildPolicy(ChildPolicy.TERMINATE)

        WorkflowConfig.client.registerWorkflowType(registerRequest)
      }
    }
  }
}

//this could come from another system, like BX sweep
object PrototypeWorkflowExecutor {
  def main(args: Array[String]) {

    val executionRequest: StartWorkflowExecutionRequest = new StartWorkflowExecutionRequest()
    executionRequest.setDomain(WorkflowConfig.domain)
    executionRequest.setTaskList(PrototypeDeciderConfig.taskList)//todo: can leave as default for workflow type?
    executionRequest.setWorkflowId("Test1")//todo: name according to something unique, like order id. Must only be unique among running workflows
    executionRequest.setWorkflowType(PrototypeWorkflowExecutorConfig.workflowType)
    executionRequest.setInput("Here is some workflow input")//todo: get from command line or something

    WorkflowConfig.client.startWorkflowExecution(executionRequest)
  }
}
