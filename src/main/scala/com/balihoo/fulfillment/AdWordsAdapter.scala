package com.balihoo.fulfillment

import scala.language.implicitConversions

import com.balihoo.fulfillment.config.PropertiesLoader
import com.google.api.ads.adwords.lib.client.AdWordsSession
import com.google.api.client.auth.oauth2.Credential
import com.google.api.ads.common.lib.auth.OfflineCredentials.Api
import com.google.api.ads.common.lib.auth.OfflineCredentials
import org.apache.commons.configuration.{BaseConfiguration, Configuration}
import com.google.api.ads.adwords.axis.factory.AdWordsServices
import com.google.api.ads.adwords.axis.v201402.cm._
import com.google.api.ads.adwords.axis.v201402.mcm._
import scala.collection.mutable

class AdWordsAdapter(loader: PropertiesLoader) {
  val config = loader

  private val configuration:Configuration = new BaseConfiguration()
  configuration.addProperty("api.adwords.refreshToken", loader.getString("refreshToken"))
  configuration.addProperty("api.adwords.clientId", loader.getString("clientId"))
  configuration.addProperty("api.adwords.clientSecret", loader.getString("clientSecret"))

  val baseAccountId = loader.getString("baseAccountId")

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
    .withDeveloperToken(loader.getString("developerToken"))
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

class RateExceededException(e:RateExceededError) extends Exception {
  val error = e
}

