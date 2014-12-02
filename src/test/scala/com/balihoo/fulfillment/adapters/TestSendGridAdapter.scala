package com.balihoo.fulfillment.adapters

import org.joda.time.DateTime
import org.specs2.mock.Mockito
import org.specs2.mutable._
import org.specs2.runner._
import org.specs2.specification.Scope
import com.balihoo.fulfillment.config.{PropertiesLoader, PropertiesLoaderComponent}
import com.balihoo.fulfillment.util.{Splogger, SploggerComponent}
import com.stackmob.newman.response.{HttpResponse, HttpResponseCode}
import org.apache.commons.codec.digest.DigestUtils
import org.junit.runner.RunWith
import java.net.URL
import com.netaporter.uri.dsl._
import play.api.libs.json.{JsObject, Json}

@RunWith(classOf[JUnitRunner])
class TestSendGridAdapter extends Specification with Mockito {
  "SendGridAdapter" should {
    "verify that a real subaccount exists" in new Adapter {
      httpAdapterGet returns buildHttpResponse(200,
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
      sendGridAdapter.checkSubaccountExists(realSubaccountId) must beSome(realSubaccountCredentials.apiUser)
      there was one(_httpAdapter).get(===(profileGetUrl), ===(basicQueryParams(realSubaccountCredentials)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "verify that a test subaccount exists" in new Adapter {
      httpAdapterGet returns buildHttpResponse(200,
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
      sendGridAdapter.checkSubaccountExists(testSubaccountId) must beSome(testSubaccountUser)
      there was one(_httpAdapter).get(===(profileGetUrl), ===(basicQueryParams(testSubAccountCredentials)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "verify that a bogus subaccount doesn't exist" in new Adapter {
      httpAdapterGet returns permissionErrorResponse
      sendGridAdapter.checkSubaccountExists(bogusSubaccountId) must beNone
      there was one(_httpAdapter).get(===(profileGetUrl), ===(basicQueryParams(bogusSubaccountCredentials)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "handle a bad response when checking for a subaccount" in new Adapter {
      httpAdapterGet returns serverErrorResponse
      sendGridAdapter.checkSubaccountExists(errorSubaccountId1) must throwA[SendGridException]
      there was one(_httpAdapter).get(===(profileGetUrl), ===(basicQueryParams(errorSubaccount1Credentials)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "create a subaccount" in new Adapter {
      httpAdapterGet returns successResponse
      sendGridAdapter.createSubaccount(realSubaccount)
      there was one(_httpAdapter).get(===(createSubaccountUrl), ===(accountCreationQueryParams(realSubaccount)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "handle a bad response when creating a subaccount" in new Adapter {
      httpAdapterGet returns permissionErrorResponse
      sendGridAdapter.createSubaccount(errorSubaccount1) must throwA[SendGridException]
      there was one(_httpAdapter).get(===(createSubaccountUrl), ===(accountCreationQueryParams(errorSubaccount1)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "handle an error response when creating a subaccount" in new Adapter {
      httpAdapterGet returns serverErrorResponse
      sendGridAdapter.createSubaccount(errorSubaccount2) must throwA[SendGridException]
      there was one(_httpAdapter).get(===(createSubaccountUrl), ===(accountCreationQueryParams(errorSubaccount2)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "update a subaccount profile" in new Adapter {
      httpAdapterGet returns successResponse
      sendGridAdapter.updateProfile(realSubaccount)
      there was one(_httpAdapter).get(===(subaccountProfileUrl), ===(profileUpdateQueryParams(realSubaccount)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "handle a bad response when updating a subaccount profile" in new Adapter {
      httpAdapterGet returns permissionErrorResponse
      sendGridAdapter.updateProfile(errorSubaccount1) must throwA[SendGridException]
      there was one(_httpAdapter).get(===(subaccountProfileUrl), ===(profileUpdateQueryParams(errorSubaccount1)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "handle an error response when updating a subaccount profile" in new Adapter {
      httpAdapterGet returns serverErrorResponse
      sendGridAdapter.updateProfile(errorSubaccount2) must throwA[SendGridException]
      there was one(_httpAdapter).get(===(subaccountProfileUrl), ===(profileUpdateQueryParams(errorSubaccount2)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "activate an app" in new Adapter {
      httpAdapterGet returns successResponse
      sendGridAdapter.activateApp(realSubaccountCredentials.apiUser, appName)
      there was one(_httpAdapter).get(===(subaccountAppsUrl), ===(appActivationQueryParams(realSubaccount)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "handle a bad response when activating an app" in new Adapter {
      httpAdapterGet returns permissionErrorResponse
      sendGridAdapter.activateApp(errorSubaccount1Credentials.apiUser, appName) must throwA[SendGridException]
      there was one(_httpAdapter).get(===(subaccountAppsUrl), ===(appActivationQueryParams(errorSubaccount1)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "handle an error response when activating an app" in new Adapter {
      httpAdapterGet returns serverErrorResponse
      sendGridAdapter.activateApp(errorSubaccount2Credentials.apiUser, appName) must throwA[SendGridException]
      there was one(_httpAdapter).get(===(subaccountAppsUrl), ===(appActivationQueryParams(errorSubaccount2)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "configure the event notification app" in new Adapter {
      httpAdapterGet returns successResponse
      sendGridAdapter.configureEventNotificationApp(realSubaccountCredentials.apiUser, webhookUrl, webhookUsername, webhookPassword)
      there was one(_httpAdapter).get(===(subaccountAppsUrl), ===(eventNotifyConfigQueryParams(realSubaccount)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "handle a bad response when configuring the event notification app" in new Adapter {
      httpAdapterGet returns permissionErrorResponse
      sendGridAdapter.configureEventNotificationApp(errorSubaccount1Credentials.apiUser, webhookUrl, webhookUsername, webhookPassword) must throwA[SendGridException]
      there was one(_httpAdapter).get(===(subaccountAppsUrl), ===(eventNotifyConfigQueryParams(errorSubaccount1)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "handle an error response when configuring the event notification app" in new Adapter {
      httpAdapterGet returns serverErrorResponse
      sendGridAdapter.configureEventNotificationApp(errorSubaccount2Credentials.apiUser, webhookUrl, webhookUsername, webhookPassword) must throwA[SendGridException]
      there was one(_httpAdapter).get(===(subaccountAppsUrl), ===(eventNotifyConfigQueryParams(errorSubaccount2)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "set an IP address" in new Adapter {
      httpAdapterGet returns successResponse
      sendGridAdapter.setIpAddress(realSubaccountCredentials.apiUser, ipAddress)
      there was one(_httpAdapter).get(===(subaccountSendIpUrl), ===(setIpAddressQueryParams(realSubaccount)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "handle a bad response when setting an IP address" in new Adapter {
      httpAdapterGet returns permissionErrorResponse
      sendGridAdapter.setIpAddress(errorSubaccount1Credentials.apiUser, ipAddress) must throwA[SendGridException]
      there was one(_httpAdapter).get(===(subaccountSendIpUrl), ===(setIpAddressQueryParams(errorSubaccount1)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "handle an error response when setting an IP address" in new Adapter {
      httpAdapterGet returns serverErrorResponse
      sendGridAdapter.setIpAddress(errorSubaccount2Credentials.apiUser, ipAddress) must throwA[SendGridException]
      there was one(_httpAdapter).get(===(subaccountSendIpUrl), ===(setIpAddressQueryParams(errorSubaccount2)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "set a whitelabel" in new Adapter {
      httpAdapterGet returns successResponse
      sendGridAdapter.setWhitelabel(realSubaccountCredentials.apiUser, whitelabel)
      there was one(_httpAdapter).get(===(subaccountWhitelabelUrl), ===(setWhitelabelQueryParams(realSubaccount)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "handle a bad response when setting a whitelabel" in new Adapter {
      httpAdapterGet returns permissionErrorResponse
      sendGridAdapter.setWhitelabel(errorSubaccount1Credentials.apiUser, whitelabel) must throwA[SendGridException]
      there was one(_httpAdapter).get(===(subaccountWhitelabelUrl), ===(setWhitelabelQueryParams(errorSubaccount1)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "handle an error response when setting a whitelabel" in new Adapter {
      httpAdapterGet returns serverErrorResponse
      sendGridAdapter.setWhitelabel(errorSubaccount2Credentials.apiUser, whitelabel) must throwA[SendGridException]
      there was one(_httpAdapter).get(===(subaccountWhitelabelUrl), ===(setWhitelabelQueryParams(errorSubaccount2)), any[Seq[(String, String)]], any[Option[(String, String)]])
    }

    "format recipient data correctly" in new Adapter {
      val recipientCsv = Stream(
        List("Name", "Email", "RecipientID", "FavoriteCereal", "SoonToBeDiabetic"),
        List("Test1", "test1@balihoo.com", "t1", "Wheaties", "No"),
        List("Test2", "test2@balihoo.com", "t2", "Corn Flakes", "No"),
        List("Test3", "test3@balihoo.com", "t3", "Chocolate Frosted Sugar Bombs", "Yes")) // http://www.gocomics.com/calvinandhobbes/1986/03/22
      val result = sendGridAdapter.testFormatRecipientData(uniqueArgs, emailSendTime, recipientCsv, recipientIdHeading, emailHeading)
      result.value("to").as[List[String]] === List("test1@balihoo.com", "test2@balihoo.com", "test3@balihoo.com")
      result.value("unique_args").as[Map[String, String]] === Map("campaignId" -> "grilled donuts", "tacticId" -> "oak tree", "locationId" -> "somewhere", "recipientId" -> "[RECIPIENT ID]")
      result.value("sub").as[Map[String, List[String]]] === Map(
        "[RECIPIENT ID]" -> List("t1", "t2", "t3"),
        "%%Name%%" -> List("Test1", "Test2", "Test3"),
        "%%Email%%" -> List("test1@balihoo.com", "test2@balihoo.com", "test3@balihoo.com"),
        "%%RecipientID%%" -> List("t1", "t2", "t3"),
        "%%FavoriteCereal%%" -> List("Wheaties", "Corn Flakes", "Chocolate Frosted Sugar Bombs"),
        "%%SoonToBeDiabetic%%" -> List("No", "No", "Yes"))
    }

    "complain when recipient data is empty" in new Adapter {
      val recipientCsv = Stream(
        List("Name", "Email", "RecipientID", "FavoriteCereal", "SoonToBeDiabetic"))
      sendGridAdapter.testFormatRecipientData(uniqueArgs, emailSendTime, recipientCsv, recipientIdHeading, emailHeading) must throwA[SendGridException](message = "The recipient list is empty.")
    }

    "complain when recipient ID is missing from recipient data" in new Adapter {
      val recipientCsv = Stream(
        List("Name", "Email", "FavoriteCereal"),
        List("Test1", "test1@balihoo.com", "Wheaties"))
      sendGridAdapter.testFormatRecipientData(uniqueArgs, emailSendTime, recipientCsv, recipientIdHeading, emailHeading) must throwA[SendGridException](message = "Missing recipientId column")
    }

    "complain when email is missing from recipient data" in new Adapter {
      val recipientCsv = Stream(
        List("Name", "RecipientID", "FavoriteCereal"),
        List("Test1", "t1", "Wheaties"))
      sendGridAdapter.testFormatRecipientData(uniqueArgs, emailSendTime, recipientCsv, recipientIdHeading, emailHeading) must throwA[SendGridException](message = "Missing email column")
    }

    "complain when recipient data is a jagged array" in new Adapter {
      val recipientCsv = Stream(
        List("Name", "Email", "RecipientID", "FavoriteCereal"),
        List("Test1", "test1@balihoo.com", "t1", "Wheaties"),
        List("Test2", "test2@balihoo.com", "t2", "Corn Flakes"),
        List("Test3", "test3@balihoo.com", "t3"))
      val result = sendGridAdapter.testFormatRecipientData(uniqueArgs, emailSendTime, recipientCsv, recipientIdHeading, emailHeading) must throwA[SendGridException](
        message = "Unable to transpose recipient data. Please make sure all rows are the same length.")
    }

    "send an email" in new Adapter {
      httpAdapterPost returns successResponse
      sendGridAdapter.sendEmail(testSubAccountCredentials, uniqueArgs, emailSendTime, email, recipientCsv, recipientIdHeading, emailHeading)
      there was one(_httpAdapter).post(===(emailSendUrl), ===(emailSendBody), any[Seq[(String, Any)]], ===(emailSendHeaders), any[Option[(String, String)]])
    }

    "handle a bad response when sending an email" in new Adapter {
      httpAdapterPost returns permissionErrorResponse
      sendGridAdapter.sendEmail(testSubAccountCredentials, uniqueArgs, emailSendTime, email, recipientCsv, recipientIdHeading, emailHeading) must throwA[SendGridException]
      there was one(_httpAdapter).post(===(emailSendUrl), ===(emailSendBody), any[Seq[(String, Any)]], ===(emailSendHeaders), any[Option[(String, String)]])
    }

    "handle an error response when sending an email" in new Adapter {
      httpAdapterPost returns serverErrorResponse
      sendGridAdapter.sendEmail(testSubAccountCredentials, uniqueArgs, emailSendTime, email, recipientCsv, recipientIdHeading, emailHeading) must throwA[SendGridException]
      there was one(_httpAdapter).post(===(emailSendUrl), ===(emailSendBody), any[Seq[(String, Any)]], ===(emailSendHeaders), any[Option[(String, String)]])
    }
  }

  val testRootApiUser = "mainAccountUser"
  val testRootApiKey = "StinkyCheese"
  val testSubaccountUser = "drunkenGorilla"
  val testPasswordSalt = "iodized"
  val v1ApiBaseUrlString = "https://whats.up/doc"
  // Don't put a slash at the end of this URL, or the tests won't work.
  val v2ApiBaseUrlString = "https://less.old/api"
  // Same with this one.
  val v3ApiBaseUrlString = "https://hot.new/api"
  // Same with this one.
  val profileGetUrl = new URL(v1ApiBaseUrlString / "profile.get.json")
  val createSubaccountUrl = new URL(v2ApiBaseUrlString / "customer.add.json")
  val subaccountProfileUrl = new URL(v1ApiBaseUrlString / "profile.set.json")
  val subaccountAppsUrl = new URL(v2ApiBaseUrlString / "customer.apps.json")
  val subaccountSendIpUrl = new URL(v2ApiBaseUrlString / "customer.sendip.json")
  val subaccountWhitelabelUrl = new URL(v2ApiBaseUrlString / "customer.whitelabel.json")
  val emailSendUrl = new URL(v1ApiBaseUrlString / "mail.send.json")
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
  val webhookUrl = new URL("https://listens.to.everything/all/the/time")
  val webhookUsername = "not_root@domain"
  val webhookPassword = "secret password"
  val webhookUrlParam = "https://not_root%40domain:secret%20password@listens.to.everything/all/the/time"
  val ipAddress = "10.9.8.7"
  val whitelabel = "test.balihoo.com"
  val uniqueArgs = Json.obj("campaignId" -> "grilled donuts", "tacticId" -> "oak tree", "locationId" -> "somewhere")
  val emailSendTime = new DateTime(1416867699433L)
  val emailSendTimestamp = 1416867699 // The UNIX timestamp version of emailSendTime
  val emailSendHeaders = Seq("Content-Type" -> "application/x-www-form-urlencoded")
  val recipientIdHeading = "recipientId"
  val emailHeading = "email"
  val email = Email(
    fromAddress = "sclaus@north.pole",
    fromName = "Santa Claus",
    replyToAddress = "noreply@naughtylist.north.pole",
    subject = "You better watch out",
    body = "<html><body>You asked for a %%other%%.  Be good.</body></html>")
  val recipientCsv = Stream(
    List("recipientid", "email", "want"),
    List("r1", "r1@domain", "widget"),
    List("r2", "r2@domain", "gadget"))
  val formattedRecipientData = Json.obj(
    "to" -> Json.arr("r1@domain", "r2@domain"),
    "sub" -> Json.obj(
      "[RECIPIENT ID]" -> Json.arr("r1", "r2"),
      "%%recipientid%%" -> Json.arr("r1", "r2"),
      "%%email%%" -> Json.arr("r1@domain", "r2@domain"),
      "%%want%%" -> Json.arr("widget", "gadget")),
    "unique_args" -> Json.obj("campaignId" -> "grilled donuts", "tacticId" -> "oak tree", "locationId" -> "somewhere", "recipientId" -> "[RECIPIENT ID]"),
    "send_at" -> Json.toJson(emailSendTimestamp)).toString
  val emailSendBody = HTTPAdapter.encodeFormData(Seq(
      "to" -> "test@balihoo.com",
      "subject" -> email.subject,
      "html" -> email.body,
      "from" -> email.fromAddress,
      "fromName" -> email.fromName,
      "replyTo" -> email.replyToAddress,
      "x-smtpapi" -> formattedRecipientData,
      "api_user" -> testSubAccountCredentials.apiUser,
      "api_key" -> testSubAccountCredentials.apiKey))

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
    """.
      stripMargin)

  val permissionErrorResponse = buildHttpResponse(200,
    """
      |{
      |  "error": {
      |    "code": 401,
      |    "message": "Permission denied, wrong credentials"
      |  }
      |}
    """.
      stripMargin)

  val serverErrorResponse = buildHttpResponse(500, "Server is drunk")

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
    basicQueryParams(apiCredentials,  Seq(("task", "append"), ("user", subaccount.credentials.apiUser), ("mail_domain", whitelabel)))
  }

  /**
   *Factory method for mock HttpResponse objects
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

  /**
   * Creates SendGridCredentials for a given username
   * @param apiUser
   * @return
   */
  def apiUserToCredentials(apiUser: String): SendGridCredentials = {
    val apiKey = DigestUtils.sha256Hex(apiUser + testPasswordSalt).substring(0, 16);
    SendGridCredentials(apiUser, apiKey)
  }

  trait Adapter extends Scope {
    val _httpAdapter = mock[HTTPAdapter]
    def httpAdapterGet = _httpAdapter.get(any[URL], any[Seq[(String, Any)]], any[Seq[(String, String)]], any[Option[(String, String)]])
    def httpAdapterPost = _httpAdapter.post(any[URL], any[String], any[Seq[(String, Any)]], any[Seq[(String, String)]], any[Option[(String, String)]])

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

      val httpAdapter = _httpAdapter

      /**
       * Exposing this method for testing.
       */
      def testFormatRecipientData(uniqueArgs: JsObject, sendTime: DateTime, recipientCsv: Stream[List[String]], recipientIdHeading: String, emailHeading: String) =
        formatRecipientData(uniqueArgs, sendTime, recipientCsv, recipientIdHeading, emailHeading)
    }
  }
}
