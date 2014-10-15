package com.balihoo.fulfillment.adapters

import com.amazonaws.{AmazonServiceException, AmazonClientException}
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.amazonaws.services.simpleworkflow.model._
import com.balihoo.fulfillment.config._

//for the cake pattern dependency injection
trait SWFAdapterComponent {
  def swfAdapter: AbstractSWFAdapter with PropertiesLoaderComponent
}

abstract class AbstractSWFAdapter extends AWSAdapter[AmazonSimpleWorkflowAsyncClient] {
  this: PropertiesLoaderComponent =>

  val workflowName = new SWFName(config.getString("workflowName"))
  val workflowVersion = new SWFVersion(config.getString("workflowVersion"))
  val workflowTaskListName = new SWFName(workflowName+workflowVersion)

  def verifyDomain(autoRegister:Boolean = false) = {
    val ddr = new DescribeDomainRequest
    ddr.setName(domain)

    try {
      println(s"Checking for domain '$domain'..")
      client.describeDomain(ddr)
      println("Found it!")
    } catch {
      case ure:UnknownResourceException =>
        println(s"The domain '$domain' doesn't exist!")
        if(autoRegister) { registerDomain() }
    }
  }

  def registerDomain() = {
    val rdr = new RegisterDomainRequest
    rdr.setName(domain)
    rdr.setWorkflowExecutionRetentionPeriodInDays(config.getString("workflowRetentionDays"))
    rdr.setDescription(config.getString("domainDescription"))

    try {
      println(s"Trying to register domain '$domain'")
      client.registerDomain(rdr)
    } catch {
      case daee:DomainAlreadyExistsException =>
        // Someone beat us to it! No worries..
      case lee:LimitExceededException =>
        // We've registered too many domains!?
        throw new Exception("Can't register new domain!", lee)
      //      case onpe:OperationNotPermittedException => FATAL DON'T CATCH
      //      case ace:AmazonClientException => FATAL DON'T CATCH
      //      case ase:AmazonServiceException => FATAL DON'T CATCH
      //      case e:Exception => FATAL DON'T CATCH
    }
  }

  def verifyWorkflowType(autoRegister:Boolean = false) = {
    val wt = new WorkflowType
    wt.setName(workflowName)
    wt.setVersion(workflowVersion)
    val wtr = new DescribeWorkflowTypeRequest
    wtr.setDomain(domain)
    wtr.setWorkflowType(wt)

    try {
      println(s"Checking for workflow type '$workflowName:$workflowVersion'..")
      client.describeWorkflowType(wtr)
      println("Found it!")
    } catch {
      case ure:UnknownResourceException =>
        println(s"The workflow type '${wt.getName}:${wt.getVersion}' doesn't exist!")
        if(autoRegister) { registerWorkflowType() }

    }
  }

  def registerWorkflowType() = {
    val taskList = new TaskList
    taskList.setName(workflowTaskListName)
    val rwtr = new RegisterWorkflowTypeRequest
    rwtr.setDomain(domain)
    rwtr.setName(workflowName)
    rwtr.setVersion(workflowVersion)
    rwtr.setDefaultTaskList(taskList)

    try {
      println(s"Trying to register workflow type '$workflowName:$workflowVersion'")
      client.registerWorkflowType(rwtr)
    } catch {
      case taee:TypeAlreadyExistsException =>
        // Someone beat us to it! No worries..
      case lee:LimitExceededException =>
        // We've registered too many workflow types!?
        throw new Exception("Can't register new workflow type!", lee)
      //      case onpe:OperationNotPermittedException =>
      //      case ure:UnknownResourceException =>
      //      case ace:AmazonClientException =>
      //      case ase:AmazonServiceException =>
      //      case e:Exception =>

    }
  }
}

class SWFAdapter(cfg: PropertiesLoader, autoRegister:Boolean = false) extends AbstractSWFAdapter with PropertiesLoaderComponent {
  def config = cfg

  verifyDomain(autoRegister)
  verifyWorkflowType(autoRegister)
}
