package com.balihoo.fulfillment.adapters

import java.net.URL

import com.google.api.ads.adwords.lib.utils.v201409.ReportDownloader

import scala.language.implicitConversions

import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.lib.client.AdWordsSession
import com.google.api.client.auth.oauth2.Credential
import com.google.api.ads.common.lib.auth.OfflineCredentials.Api
import com.google.api.ads.common.lib.auth.OfflineCredentials
import org.apache.commons.configuration.{BaseConfiguration, Configuration}
import com.google.api.ads.adwords.axis.factory.AdWordsServices
import com.google.api.ads.adwords.axis.v201409.cm._
import com.google.api.ads.adwords.axis.v201409.mcm._
import scala.collection.mutable
import scala.sys.process._
import scala.util.matching.Regex

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
    .build()

  session.setValidateOnly(false)

  private val services = new AdWordsServices

  val campaignService:CampaignServiceInterface = services.get(session, classOf[CampaignServiceInterface])
  val campaignCriterionService:CampaignCriterionServiceInterface = services.get(session, classOf[CampaignCriterionServiceInterface])
  val campaignAdExtensionService:CampaignAdExtensionServiceInterface = services.get(session, classOf[CampaignAdExtensionServiceInterface])
  val adGroupService:AdGroupServiceInterface = services.get(session, classOf[AdGroupServiceInterface])
  val adGroupCriterionService:AdGroupCriterionServiceInterface = services.get(session, classOf[AdGroupCriterionServiceInterface])
  val adGroupBidModifierService:AdGroupBidModifierServiceInterface = services.get(session, classOf[AdGroupBidModifierServiceInterface])
  val adGroupAdService:AdGroupAdServiceInterface = services.get(session, classOf[AdGroupAdServiceInterface])
  val managedCustomerService:ManagedCustomerServiceInterface = services.get(session, classOf[ManagedCustomerServiceInterface])
  val budgetService:BudgetServiceInterface = services.get(session, classOf[BudgetServiceInterface])
  val locationService:LocationCriterionServiceInterface = services.get(session, classOf[LocationCriterionServiceInterface])
  val mediaService:MediaServiceInterface = services.get(session, classOf[MediaServiceInterface])
  val constantDataService:ConstantDataServiceInterface = services.get(session, classOf[ConstantDataServiceInterface])
  val geoLocationService:GeoLocationServiceInterface = services.get(session, classOf[GeoLocationServiceInterface])
  val reportDefinitionService:ReportDefinitionServiceInterface = services.get(session, classOf[ReportDefinitionServiceInterface])
  val reportDownloader:ReportDownloader = new ReportDownloader(session)

  def dollarsToMicros(dollars:Float):Long = {
    (dollars * 1000000).toLong
  }

  def microsToDollars(micros:Long):Float = {
    micros / 1000000.0f
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
              Thread.sleep(rateExceeded.getRetryAfterSeconds * 1200) // 120% of the the recommended wait time
              throw new RateExceededException(rateExceeded)
            case apiError: ApiError =>
              errors += (apiError.getErrorString + "(" + apiError.getTrigger + ") path:"
                + apiError.getFieldPath + " where:" + context)
            case _ =>
              errors += error.getErrorString + " " + context

          }
        }
        throw new Exception(s"${errors.length} Errors!: " + errors.mkString("\n"))
    }
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

object AdWordsPolicy {

  def escapeSpaces(text:String):String = {
    text.replace(" ", "%20")
  }

  def noProtocol(text:String):String = {
    text.replace("http://", "").replace("https://", "")
  }

  def addProtocol(text:String, protocol:String = "http"):String = {
    s"$protocol://${noProtocol(text)}"
  }

  def noWWW(text:String):String = {
    noProtocol(text.replaceFirst("""^www\.""", ""))
  }

  def cleanUrl(url:String):String = {
    val testUrl = s"curl -Is $url --max-time 2 --retry 3"
    try {
      testUrl.!!
      addProtocol(url)
    } catch {
      case e:Exception =>
        throw new Exception(s"URL:$url does NOT resolve!")
        "URL_DOES_NOT_RESOLVE"
    }
  }

  def displayUrl(url:String):String = {
    limitString(noWWW(url), 255)
  }

  def destinationUrl(url:String):String = {
    limitString(cleanUrl(url), 2047)
  }

  /**
   *
   * @param displayUrl String
   * @param destUrl String
   */
  def matchDomains(displayUrl:String, destUrl:String) = {
    val dispHost = new URL(addProtocol(displayUrl)).getHost
    val destHost = new URL(addProtocol(destUrl)).getHost
    if(dispHost != destHost) {
      throw new Exception(s"Domains for destination and display URLs must match! ($destHost =/= $dispHost)")
    }
    true
  }

  def fixUpperCaseViolations(text:String):String = {
    val upperMatcher = new Regex("""[A-Z]{2}""", "token")
    (for(part:String <- text.split("""\s+""") if part.length > 0)
    yield upperMatcher.findFirstIn(part) match { // check to see if we match two consecutive upper case letters...
        case d:Some[String] =>
          part.toLowerCase.capitalize
        case None =>
          part
      }
      ).mkString(" ")
  }

  /**
   * AdWords has lots of rules related to string length
   * @param text String
   * @param maxLength Int
   * @return
   */
  def limitString(text:String, maxLength:Int):String = {
    if(text.length > maxLength) {
      throw new Exception(s"'$text' is too long! (max $maxLength)")
    }

    text
  }

  /**
   *
   * https://developers.google.com/adwords/api/docs/reference/v201409/AdGroupCriterionService.Keyword
   * @param text String
   */
  def validateKeyword(text:String):String = {
    if(text.length > 80) {
      throw new Exception(s"Keyword '$text' is too long! (max 80)")
    }

    if(text.split("""\s+""").length > 10) {
      throw new Exception(s"Keyword '$text' has too many words! (max 10)")
    }

    text
  }

  def removeIllegalCharacters(text:String):String = {
    text.replaceAll("""[^a-zA-Z0-9\s\.&-+]""", "")
  }

}

