package com.balihoo.fulfillment.adapters

import com.netaporter.uri.config.UriConfig
import com.netaporter.uri.parsing.UriParser
import org.specs2.matcher.AnyMatchers._
import org.specs2.mock._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Scope
import com.balihoo.fulfillment.config.{PropertiesLoader, PropertiesLoaderComponent}
import com.balihoo.fulfillment.util.{Splogger, SploggerComponent}
import com.stackmob.newman.response.{HttpResponse, HttpResponseCode}
import org.apache.commons.codec.digest.DigestUtils
import org.junit.runner.RunWith
import java.net.URL
import com.netaporter.uri.dsl._

@RunWith(classOf[JUnitRunner])
class TestSendGridAdapter extends Specification {
  "SendGridAdapter" should {
    "verify that a real subaccount exists" in new adapter {
      sendGridAdapter.checkSubaccountExists(realSubaccountId) must beSome(realSubaccountCredentials.apiUser)
    }

    "verify that a test subaccount exists" in new adapter {
      sendGridAdapter.checkSubaccountExists(testSubaccountId) must beSome(testSubaccountUser)
    }

    "verify that a bogus subaccount doesn't exist" in new adapter {
      sendGridAdapter.checkSubaccountExists(bogusSubaccountId) must beNone
    }

    "handle a bad response when checking for a subaccount" in new adapter {
      sendGridAdapter.checkSubaccountExists(errorSubaccountId1) must throwA[SendGridException]
    }

    "create a subaccount" in new adapter {
      sendGridAdapter.createSubaccount(realSubaccount)
    }

    "handle a bad response when creating a subaccount" in new adapter {
      sendGridAdapter.createSubaccount(errorSubaccount1) must throwA[SendGridException]
    }

    "handle an error response when creating a subaccount" in new adapter {
      sendGridAdapter.createSubaccount(errorSubaccount2) must throwA[SendGridException]
    }

    "update a subaccount profile" in new adapter {
      sendGridAdapter.updateProfile(realSubaccount)
    }

    "handle a bad response when updating a subaccount profile" in new adapter {
      sendGridAdapter.updateProfile(errorSubaccount1) must throwA[SendGridException]
    }

    "handle an error response when updating a subaccount profile" in new adapter {
      sendGridAdapter.updateProfile(errorSubaccount2) must throwA[SendGridException]
    }

    "activate an app" in new adapter {
      sendGridAdapter.activateApp(realSubaccountCredentials.apiUser, appName)
    }

    "handle a bad response when activating an app" in new adapter {
      sendGridAdapter.activateApp(errorSubaccount1Credentials.apiUser, appName) must throwA[SendGridException]
    }

    "handle an error response when activating an app" in new adapter {
      sendGridAdapter.activateApp(errorSubaccount2Credentials.apiUser, appName) must throwA[SendGridException]
    }

    "configure the event notification app" in new adapter {
      sendGridAdapter.configureEventNotificationApp(realSubaccountCredentials.apiUser, webhookUrl, webhookUsername, webhookPassword)
    }

    "handle a bad response when configuring the event notification app" in new adapter {
      sendGridAdapter.configureEventNotificationApp(errorSubaccount1Credentials.apiUser, webhookUrl, webhookUsername, webhookPassword) must throwA[SendGridException]
    }

    "handle an error response when configuring the event notification app" in new adapter {
      sendGridAdapter.configureEventNotificationApp(errorSubaccount2Credentials.apiUser, webhookUrl, webhookUsername, webhookPassword) must throwA[SendGridException]
    }

    "set an IP address" in new adapter {
      sendGridAdapter.setIpAddress(realSubaccountCredentials.apiUser, ipAddress)
    }

    "handle a bad response when setting an IP address" in new adapter {
      sendGridAdapter.setIpAddress(errorSubaccount1Credentials.apiUser, ipAddress) must throwA[SendGridException]
    }

    "handle an error response when setting an IP address" in new adapter {
      sendGridAdapter.setIpAddress(errorSubaccount2Credentials.apiUser, ipAddress) must throwA[SendGridException]
    }

    "set a whitelabel" in new adapter {
      sendGridAdapter.setWhitelabel(realSubaccountCredentials.apiUser, whitelabel)
    }

    "handle a bad response when setting a whitelabel" in new adapter {
      sendGridAdapter.setWhitelabel(errorSubaccount1Credentials.apiUser, whitelabel) must throwA[SendGridException]
    }

    "handle an error response when setting a whitelabel" in new adapter {
      sendGridAdapter.setWhitelabel(errorSubaccount2Credentials.apiUser, whitelabel) must throwA[SendGridException]
    }
  }
}

trait adapter extends Scope with Mockito {
  val testRootApiUser = "mainAccountUser"
  val testRootApiKey = "StinkyCheese"
  val testSubaccountUser = "drunkenGorilla"
  val testPasswordSalt = "iodized"
  val v1ApiBaseUrlString = "https://whats.up/doc" // Don't put a slash at the end of this URL, or the tests won't work.
  val v2ApiBaseUrlString = "https://less.old/api" // Same with this one.
  val v3ApiBaseUrlString = "https://hot.new/api" // Same with this one.
  val profileGetUrl = new URL(v1ApiBaseUrlString / "profile.get.json")
  val createSubaccountUrl = new URL(v2ApiBaseUrlString / "customer.add.json")
  val subaccountProfileUrl = new URL(v1ApiBaseUrlString / "profile.set.json")
  val subaccountAppsUrl = new URL(v2ApiBaseUrlString / "customer.apps.json")
  val subaccountSendIpUrl = new URL(v2ApiBaseUrlString / "customer.sendip.json")
  val subaccountWhitelabelUrl = new URL(v2ApiBaseUrlString / "customer.whitelabel.json")
  val realSubaccountParticipantId = "12345"
  val bogusSubaccountParticipantId = "manEatingBanana"
  val errorSubaccountParticipantId1 = "indigestion1"
  val errorSubaccountParticipantId2 = "indigestion2"
  val realSubaccountId = SendGridSubaccountId(realSubaccountParticipantId, false)
  val testSubaccountId = SendGridSubaccountId(realSubaccountParticipantId, true)
  val bogusSubaccountId = SendGridSubaccountId(bogusSubaccountParticipantId, false)
  val errorSubaccountId1 = SendGridSubaccountId(errorSubaccountParticipantId1, false)
  val apiCredentials = SendGridCredentials(testRootApiUser, testRootApiKey)
  val testSubAccountCredentials = apiUserToCredentials(testSubaccountUser)
  val realSubaccountCredentials = apiUserToCredentials(s"FF$realSubaccountParticipantId")
  val bogusSubaccountCredentials = apiUserToCredentials(s"FF$bogusSubaccountParticipantId")
  val errorSubaccount1Credentials = apiUserToCredentials(s"FF$errorSubaccountParticipantId1")
  val errorSubaccount2Credentials = apiUserToCredentials(s"FF$errorSubaccountParticipantId2")
  val appName = "octothorpeRenderer"
  val webhookUrl = "https://listens.to.everything/all/the/time"
  val webhookUsername = "not_root@domain"
  val webhookPassword = "secret password"
  val webhookUrlParam = "https://not_root%40domain:secret%20password@listens.to.everything/all/the/time"
  val ipAddress = "10.9.8.7"
  val whitelabel = "test.balihoo.com"

  val realSubaccount = new SendGridSubaccount(
    _credentials = realSubaccountCredentials,
    _firstName = "Boba",
    _lastName = "Fett",
    _address = "1138 Imperial Way",
    _city = "Great Pit of Carkoon",
    _state = "Tatooine",
    _zip = "0982340980498",
    _country = "a galaxy far, far away",
    _phone = "867-5309")

  val testSubaccount = new SendGridSubaccount(
    _credentials = testSubAccountCredentials,
    _firstName = "Test",
    _lastName = "User",
    _address = "404 South 8th Street Suite 300",
    _city = "Boise",
    _state = "ID",
    _zip = "83702",
    _country = "USA",
    _phone = "2086294254")

  val errorSubaccount1 = new SendGridSubaccount(
    _credentials = errorSubaccount1Credentials,
    _firstName = "Test",
    _lastName = "User",
    _address = "404 South 8th Street Suite 300",
    _city = "Boise",
    _state = "ID",
    _zip = "83702",
    _country = "USA",
    _phone = "2086294254")

  val errorSubaccount2 = new SendGridSubaccount(
    _credentials = errorSubaccount2Credentials,
    _firstName = "Test",
    _lastName = "User",
    _address = "404 South 8th Street Suite 300",
    _city = "Boise",
    _state = "ID",
    _zip = "83702",
    _country = "USA",
    _phone = "2086294254")

  val successResponse = buildHttpResponse(200,
    """
      |{
      |  "message": "success"
      |}
    """.stripMargin)

  val permissionErrorResponse = buildHttpResponse(200,
    """
      |{
      |  "error": {
      |    "code": 401,
      |    "message": "Permission denied, wrong credentials"
      |  }
      |}
    """.stripMargin)

  val serverErrorResponse = buildHttpResponse(500, "Server is drunk")

  /**
   * Factory method for mock HttpResponse objects
   * @param code the HTTP response code
   * @param body the body of the response
   * @return
   */
  private def buildHttpResponse(code: Int, body: String) = {
    val responseCode = mock[HttpResponseCode]
    responseCode.code returns code

    val response = mock[HttpResponse]
    response.code returns responseCode
    response.bodyString returns body

    response
  }

  val sendGridAdapter = new AbstractSendGridAdapter
    with PropertiesLoaderComponent
    with SploggerComponent
    with HTTPAdapterComponent {

    val config = mock[PropertiesLoader]
    config.getString("apiUser") returns testRootApiUser
    config.getString("apiKey") returns testRootApiKey
    config.getString("testUser") returns testSubaccountUser
    config.getString("passwordSalt") returns testPasswordSalt
    config.getString("v1ApiBaseUrl") returns v1ApiBaseUrlString
    config.getString("v2ApiBaseUrl") returns v2ApiBaseUrlString
    config.getString("v3ApiBaseUrl") returns v3ApiBaseUrlString

    val splog = mock[Splogger]
    
    // ----------------- Begin mock HttpAdapter -------------------- //
    val httpAdapter = mock[HTTPAdapter]

    // Profile lookup
    httpAdapter.get(===(profileGetUrl), ===(basicQueryParams(testSubAccountCredentials)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns buildHttpResponse(200,
      s"""
        |[
        |  {
        |    "username": "$testSubaccountUser",
        |    "email": "$testSubaccountUser@balihoo.com",
        |    "active": "true",
        |    "first_name": "Test",
        |    "last_name": "User",
        |    "address": "404 South 8th Street Suite 300",
        |    "city": "Boise",
        |    "state": "ID",
        |    "zip": "83702",
        |    "country": "US",
        |    "phone": "2086294254",
        |    "website": "http://www.balihoo.com",
        |    "website_access": "true"
        |  }
        |]
        |""".stripMargin)
    httpAdapter.get(===(profileGetUrl), ===(basicQueryParams(realSubaccountCredentials)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns buildHttpResponse(200,
      s"""
      |[
      |  {
      |    "username": "FF$realSubaccountParticipantId",
      |    "email": "FF$realSubaccountParticipantId@balihoo.com",
      |    "active": "true",
      |    "first_name": "Test",
      |    "last_name": "User",
      |    "address": "404 South 8th Street Suite 300",
      |    "city": "Boise",
      |    "state": "ID",
      |    "zip": "83702",
      |    "country": "US",
      |    "phone": "2086294254",
      |    "website": "http://www.balihoo.com",
      |    "website_access": "true"
      |  }
      |]
      |""".stripMargin)
    httpAdapter.get(===(profileGetUrl), ===(basicQueryParams(bogusSubaccountCredentials)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns permissionErrorResponse
    httpAdapter.get(===(profileGetUrl), ===(basicQueryParams(errorSubaccount1Credentials)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns serverErrorResponse

    // Subaccount creation
    httpAdapter.get(===(createSubaccountUrl), ===(accountCreationQueryParams(realSubaccount)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns successResponse
    httpAdapter.get(===(createSubaccountUrl), ===(accountCreationQueryParams(errorSubaccount1)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns permissionErrorResponse
    httpAdapter.get(===(createSubaccountUrl), ===(accountCreationQueryParams(errorSubaccount2)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns serverErrorResponse

    // Update profile
    httpAdapter.get(===(subaccountProfileUrl), ===(profileUpdateQueryParams(realSubaccount)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns successResponse
    httpAdapter.get(===(subaccountProfileUrl), ===(profileUpdateQueryParams(errorSubaccount1)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns permissionErrorResponse
    httpAdapter.get(===(subaccountProfileUrl), ===(profileUpdateQueryParams(errorSubaccount2)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns serverErrorResponse

    // Activate app
    httpAdapter.get(===(subaccountAppsUrl), ===(appActivationQueryParams(realSubaccount)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns successResponse
    httpAdapter.get(===(subaccountAppsUrl), ===(appActivationQueryParams(errorSubaccount1)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns permissionErrorResponse
    httpAdapter.get(===(subaccountAppsUrl), ===(appActivationQueryParams(errorSubaccount2)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns serverErrorResponse

    // Configure event notification app
    httpAdapter.get(===(subaccountAppsUrl), ===(eventNotifyConfigQueryParams(realSubaccount)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns successResponse
    httpAdapter.get(===(subaccountAppsUrl), ===(eventNotifyConfigQueryParams(errorSubaccount1)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns permissionErrorResponse
    httpAdapter.get(===(subaccountAppsUrl), ===(eventNotifyConfigQueryParams(errorSubaccount2)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns serverErrorResponse

    // Set IP address
    httpAdapter.get(===(subaccountSendIpUrl), ===(setIpAddressQueryParams(realSubaccount)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns successResponse
    httpAdapter.get(===(subaccountSendIpUrl), ===(setIpAddressQueryParams(errorSubaccount1)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns permissionErrorResponse
    httpAdapter.get(===(subaccountSendIpUrl), ===(setIpAddressQueryParams(errorSubaccount2)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns serverErrorResponse

    // Set whitelabel
    httpAdapter.get(===(subaccountWhitelabelUrl), ===(setWhitelabelQueryParams(realSubaccount)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns successResponse
    httpAdapter.get(===(subaccountWhitelabelUrl), ===(setWhitelabelQueryParams(errorSubaccount1)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns permissionErrorResponse
    httpAdapter.get(===(subaccountWhitelabelUrl), ===(setWhitelabelQueryParams(errorSubaccount2)), any[Seq[(String, String)]], any[Option[(String, String)]]) returns serverErrorResponse

    // ----------------- End mock HttpAdapter -------------------- //

    /**
     * Builds a list of query parameters, starting with credentials
     * @param credentials
     * @param otherParams
     * @return
     */
    private def basicQueryParams(credentials: SendGridCredentials, otherParams: Seq[(String, Any)] = Seq()): Seq[(String, Any)] =
      Seq(("api_user", credentials.apiUser), ("api_key", credentials.apiKey)) ++ otherParams

    /**
     * Builds a list of query parameters for creating a subaccount
     * @param subaccount
     * @return
     */
    private def accountCreationQueryParams(subaccount: SendGridSubaccount): Seq[(String, Any)] = {
      basicQueryParams(apiCredentials,  Seq(("username", subaccount.credentials.apiUser),
        ("password", subaccount.credentials.apiKey), ("confirm_password", subaccount.credentials.apiKey),
        ("email", subaccount.email), ("first_name", subaccount.firstName), ("last_name", subaccount.lastName),
        ("address", subaccount.address), ("city", subaccount.city), ("state", subaccount.state), ("zip", subaccount.zip),
        ("country", subaccount.country), ("phone", subaccount.phone), ("website", "N/A")))
    }

    /**
     * Builds a list of query parameters for updating a subaccount profile
     * @param subaccount
     * @return
     */
    private def profileUpdateQueryParams(subaccount: SendGridSubaccount): Seq[(String, Any)] = {
      basicQueryParams(subaccount.credentials,  Seq(("first_name", subaccount.firstName), ("last_name", subaccount.lastName),
        ("address", subaccount.address), ("city", subaccount.city), ("state", subaccount.state),
        ("zip", subaccount.zip), ("country", subaccount.country), ("phone", subaccount.phone), ("website", "N/A")))
    }

    /**
     * Builds a list of query parameters for activating an app
     * @param subaccount
     * @return
     */
    private def appActivationQueryParams(subaccount: SendGridSubaccount): Seq[(String, Any)] = {
      basicQueryParams(apiCredentials,  Seq(("task", "activate"), ("user", subaccount.credentials.apiUser), ("name", appName)))
    }

    /**
     * Builds a list of query parameters for configuring the event notification app
     * @param subaccount
     * @return
     */
    private def eventNotifyConfigQueryParams(subaccount: SendGridSubaccount): Seq[(String, Any)] = {
      basicQueryParams(apiCredentials,  Seq(("task", "setup"), ("user", subaccount.credentials.apiUser),
        ("name", "eventnotify"), ("processed", "1"), ("dropped", "1"), ("deferred", "1"), ("delivered", "1"),
        ("bounce", "1"), ("click", "1"), ("open", "1"), ("unsubscribe", "1"), ("spamreport", "1"), ("url", webhookUrlParam)))
    }

    /**
     * Builds a list of query parameters for setting a subaccount's IP address
     * @param subaccount
     * @return
     */
    private def setIpAddressQueryParams(subaccount: SendGridSubaccount): Seq[(String, Any)] = {
      basicQueryParams(apiCredentials,  Seq(("task", "append"), ("user", subaccount.credentials.apiUser),
        ("set", "specify"), ("ip[]", ipAddress)))
    }

    /**
     * Builds a list of query parameters for setting a subaccount's whitelabel
     * @param subaccount
     * @return
     */
    private def setWhitelabelQueryParams(subaccount: SendGridSubaccount): Seq[(String, Any)] = {
      val t = basicQueryParams(apiCredentials,  Seq(("task", "append"), ("user", subaccount.credentials.apiUser),
        ("mail_domain", whitelabel)))
      t
    }
  }

  /**
   * Creates SendGridCredentials for a give username
   * @param apiUser
   * @return
   */
  def apiUserToCredentials(apiUser: String): SendGridCredentials = {
    val apiKey = DigestUtils.sha256Hex(apiUser + testPasswordSalt).substring(0, 16);
    SendGridCredentials(apiUser, apiKey)
  }

}
