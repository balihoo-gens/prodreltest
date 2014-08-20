package com.balihoo.fulfillment.adapters

import scala.language.implicitConversions

import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.lib.client.AdWordsSession
import com.google.api.client.auth.oauth2.Credential
import com.google.api.ads.common.lib.auth.OfflineCredentials.Api
import com.google.api.ads.common.lib.auth.OfflineCredentials
import org.apache.commons.configuration.{BaseConfiguration, Configuration}
import com.google.api.ads.adwords.axis.factory.AdWordsServices
import com.google.api.ads.adwords.axis.v201402.cm._
import com.google.api.ads.adwords.axis.v201402.mcm._
import scala.collection.mutable

//typical cake would nest the adapter inside the provider
// but that causes issues here for nested injection, i.e.
// where we pass components from the outer cake into an inner cake
trait AdWordsAdapterComponent {
  def adWordsAdapter: AbstractAdWordsAdapter
}

abstract class AbstractAdWordsAdapter {
  this: PropertiesLoaderComponent =>

  private val configuration:Configuration = new BaseConfiguration()
  configuration.addProperty("api.adwords.refreshToken", config.getString("refreshToken"))
  configuration.addProperty("api.adwords.clientId", config.getString("clientId"))
  configuration.addProperty("api.adwords.clientSecret", config.getString("clientSecret"))

  val baseAccountId = config.getString("baseAccountId")

  // Generate a refreshable OAuth2 credential similar to a ClientLogin token
  // and can be used in place of a service account.
  private val oAuth2Credential: Credential = new OfflineCredentials.Builder()
      .forApi(Api.ADWORDS)
      .from(configuration)
      .build()
      .generateCredential()

  // Construct an AdWordsSession.
  private val session:AdWordsSession = new AdWordsSession.Builder()
    .withOAuth2Credential(oAuth2Credential)
    .withDeveloperToken(config.getString("developerToken"))
    .withUserAgent("Balihoo_Fulfillment")
    .enableReportMoneyInMicros()
    .build()

  session.setValidateOnly(false)

  private val services = new AdWordsServices

  val campaignService:CampaignServiceInterface = services.get(session, classOf[CampaignServiceInterface])
  val campaignCriterionService:CampaignCriterionServiceInterface = services.get(session, classOf[CampaignCriterionServiceInterface])
  val adGroupService:AdGroupServiceInterface = services.get(session, classOf[AdGroupServiceInterface])
  val adGroupCriterionService:AdGroupCriterionServiceInterface = services.get(session, classOf[AdGroupCriterionServiceInterface])
  val adGroupAdService:AdGroupAdServiceInterface = services.get(session, classOf[AdGroupAdServiceInterface])
  val managedCustomerService:ManagedCustomerServiceInterface = services.get(session, classOf[ManagedCustomerServiceInterface])
  val budgedService:BudgetServiceInterface = services.get(session, classOf[BudgetServiceInterface])
  val locationService:LocationCriterionServiceInterface = services.get(session, classOf[LocationCriterionServiceInterface])
  val mediaService:MediaServiceInterface = services.get(session, classOf[MediaServiceInterface])
  val constantDataService:ConstantDataServiceInterface = services.get(session, classOf[ConstantDataServiceInterface])

  def dollarsToMicros(dollars:Float):Long = {
    (dollars * 1000000).toLong
  }

  def setClientId(id:String) = {
    session.setClientCustomerId(id)
  }

  def setValidateOnly(tf:Boolean = true) = {
    session.setValidateOnly(tf)
  }

  def setPartialFailure(tf:Boolean = true) = {
    // TODO Partial failure could be really beneficial here.
    // It might allow for all of the legal keywords in a set to work
    // and then we'd get an error about just the ones that failed, instead
    // of an all-or-nothing style of updating.
    session.setPartialFailure(tf)
  }

  def withErrorsHandled[T](context:String, code: => T):T = {
    try {
      code
    } catch {
      case apiException: ApiException =>
        val errors = new mutable.MutableList[String]()
        for((error) <- apiException.getErrors) {
          error match {
            case rateExceeded: RateExceededError =>
              throw new RateExceededException(rateExceeded)
            case apiError: ApiError =>
              errors += (apiError.getErrorString + "(" + apiError.getTrigger + ") path:"
                + apiError.getFieldPath + " where:" + context)
            case _ =>
              errors += error.getErrorString + " " + context

          }
        }
        throw new Exception(s"${errors.length} Errors!: " + errors.mkString("\n"))
      case e:Exception =>
        throw e
      case e:Throwable =>
        throw e
    }
  }

  def addOrSet(operatorId:Long): Operator = {
    if(Option(operatorId).isEmpty) Operator.ADD else Operator.SET
  }
}

class AdWordsAdapter(cfg: PropertiesLoader)
  extends AbstractAdWordsAdapter
  with PropertiesLoaderComponent {

  def config = cfg
}

class RateExceededException(e:RateExceededError) extends Exception {
  val error = e
}

