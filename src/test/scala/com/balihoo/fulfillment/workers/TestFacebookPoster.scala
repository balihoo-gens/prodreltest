package com.balihoo.fulfillment.workers

import java.net.URL
import javax.ws.rs._
import com.amazonaws.services.simpleworkflow.model.ActivityTask
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.workers.TestFacebookPoster.MockWebServer
import com.balihoo.socialmedia.facebook.FacebookConnection
import com.balihoo.socialmedia.facebook.model.Target
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.test.{TestProperties, JerseyTest}
import org.junit.runner.RunWith
import org.specs2.matcher.Matcher
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import play.api.libs.json.Json
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TestFacebookPoster extends Specification with Mockito {

  /**
   * A testable version of FacebookPoster with mocks
   * @param webServer
   */
  class TestableFacebookPoster(implicit webServer: MockWebServer)
    extends AbstractFacebookPoster
    with LoggingWorkflowAdapterTestImpl
    with FacebookAdapterComponent
    with HTTPAdapterComponent {

    val _facebookAdapter = mock[FacebookAdapter]
    _facebookAdapter.publishLinkPost(any, any, any, any, any) returns linkPostId
    _facebookAdapter.publishPhotoPost(any, any, any, any, any, any) returns photoPostId
    _facebookAdapter.publishStatusUpdate(any, any, any, any) returns statusUpdatePostId

    def facebookAdapter = _facebookAdapter

    def httpAdapter = new HTTPAdapter(1)

    val _cfg = mock[PropertiesLoader]
    _cfg.getString("geoServiceUrl") returns server.getBaseUrl.toString

    // This will change after the task completes successfully.
    var result: Option[String] = None

    // Override this method to simplify testing and to avoid swallowing exceptions.
    override def withTaskHandling(code: => String): Unit = result = Some(code)
  }

  // Implicitly convert List[Int] to java.util.List[Integer] for the sake of the Java libraries
  implicit def listOfIntToJavaListOfInteger(list: List[Int]): java.util.List[Integer] = list.map(Integer.valueOf(_))

  // Constants
  val appId = "439034opifpiojew4fpo34p9o4fok"
  val appSecret = "09340934fopijfpioj34f0t934poijwe5gpoijrfpokefa4wp9k34fvpoke4f"
  val accessToken = "98349834lkidflkj3q4f0934094flke0p9w4f4opij34f9j34f0934fmervlgkjergf5opji34op9j345io34fjio3498043to0we4rtsdrf"
  val pageId = "43984984380349034095905309354"
  val linkPostId = pageId + "_94834589258902309234092340990"
  val photoPostId = pageId + "_2398234298489040420239049493"
  val statusUpdatePostId = pageId + "_89498598324903490234939943"
  val message = "What's up?"
  val boise = "Boise,Ada,ID"
  val boiseId = TestFacebookPoster.boiseId
  val meridian = "Meridian,Ada,ID"
  val meridianId = TestFacebookPoster.meridianId
  val nampa = "Nampa,Canyon,ID"
  val nampaId = TestFacebookPoster.nampaId
  val ada = "Ada,ID"
  val canyon = "Canyon,ID"
  val idahoId = 16
  val washingtonId = 53
  val usa = "US"
  val canada = "CA"

  // Expected FacebookConnection
  val connectionMatcher: Matcher[FacebookConnection] = (c: FacebookConnection) =>
    (appId.equals(c.getAppId) && appSecret.equals(c.getAppSecret) && accessToken.equals(c.getAccessToken), "Connection doesn't match")

  // Expected Target for most tests
  val target = new Target(List(usa, canada), List(idahoId, washingtonId), List(boiseId, meridianId, nampaId))

  // Parameters that all tests have in common
  val baseParams = Map(
    "appId" -> appId,
    "appSecret" -> appSecret,
    "accessToken" -> accessToken,
    "pageId" -> pageId,
    "target" -> Json.parse(s"""{"countryCodes": ["$usa","$canada"], "regionIds": [$idahoId,$washingtonId], "cityIds": [$boiseId,$meridianId,$nampaId]}"""),
    "message" -> message)

  // Optional parameter values
  val linkUrl = "http://balihoo.com/test"
  def photoUrl = new URL(server.getBaseUrl, "photos/something.jpg").toString
  val photoName = "something.jpg"

  // Available actions
  val validateAction = "validate"
  val publishAction = "publish"

  // We'll need a web server to host a geo service and a photo.
  implicit val server = new MockWebServer

  // Fire up web server.
  step {
    server.setUp()
    success
  }

  "FacebookPoster" should {
    "validate a link post" in {
      val poster = new TestableFacebookPoster
      val params = new ActivityParameters(baseParams +("postType" -> "link", "linkUrl" -> linkUrl, "action" -> validateAction))
      poster.handleTask(params)

      there was one(poster.facebookAdapter).validateLinkPost(connectionMatcher, ===(pageId), ===(target), ===(linkUrl), ===(message))
      
      poster.result must beSome("OK")
    }

    "publish a link post" in {
      val poster = new TestableFacebookPoster
      val params = new ActivityParameters(baseParams +("postType" -> "link", "linkUrl" -> linkUrl, "action" -> publishAction))
      poster.handleTask(params)

      there was one(poster.facebookAdapter).publishLinkPost(connectionMatcher, ===(pageId), ===(target), ===(linkUrl), ===(message))

      poster.result must beSome(linkPostId)
    }

    "validate a status update" in {
      val poster = new TestableFacebookPoster
      val params = new ActivityParameters(baseParams +("postType" -> "status update", "action" -> validateAction))
      poster.handleTask(params)

      there was one(poster.facebookAdapter).validateStatusUpdate(connectionMatcher, ===(pageId), ===(target), ===(message))
     
      poster.result must beSome("OK")
    }

    "publish a status update" in {
      val poster = new TestableFacebookPoster
      val params = new ActivityParameters(baseParams +("postType" -> "status update", "action" -> publishAction))
      poster.handleTask(params)

      there was one(poster.facebookAdapter).publishStatusUpdate(connectionMatcher, ===(pageId), ===(target), ===(message))
     
      poster.result must beSome(statusUpdatePostId)
    }

    "validate a photo post" in {
      val poster = new TestableFacebookPoster
      val params = new ActivityParameters(baseParams +("postType" -> "photo", "photoUrl" -> photoUrl, "action" -> validateAction))
      poster.handleTask(params)
      
      there was one(poster.facebookAdapter).validatePhotoPost(connectionMatcher, ===(pageId), ===(target),
        ===(server.getPhotoFileContents), ===(photoName), ===(message))

      poster.result must beSome("OK")
    }

    "publish a photo post" in {
      val poster = new TestableFacebookPoster
      val params = new ActivityParameters(baseParams +("postType" -> "photo", "photoUrl" -> photoUrl, "action" -> publishAction))
      poster.handleTask(params)
      
      there was one(poster.facebookAdapter).publishPhotoPost(connectionMatcher, ===(pageId), ===(target),
        ===(server.getPhotoFileContents), ===(photoName), ===(message))
     
      poster.result must beSome(photoPostId)
    }

    "resolve subregions" in {
      val poster = new TestableFacebookPoster
      val params = new ActivityParameters(baseParams +("postType" -> "status update", "action" -> publishAction,
        "target" -> Json.parse(s"""{"countryCodes": ["$usa"], "subregions": ["$ada","$canyon"]}""")))
      poster.handleTask(params)
      
      val expectedTarget = new Target(List(usa), List(), List(boiseId, meridianId, nampaId))
      there was one(poster.facebookAdapter).publishStatusUpdate(connectionMatcher, ===(pageId), ===(expectedTarget), ===(message))
     
      poster.result must beSome(statusUpdatePostId)
    }

    "reject subregions if there are multiple countries" in {
      val poster = new TestableFacebookPoster
      val params = new ActivityParameters(baseParams +("postType" -> "status update", "action" -> publishAction,
        "target" -> Json.parse(s"""{"countryCodes": ["$usa","$canada"], "subregions": ["$ada"]}""")))

      try {
        poster.handleTask(params)
        failure("Expected an exception")
      } catch {
        case e: IllegalArgumentException => e.getMessage === "Exactly one country code is required when place names are used."
      }
      
      there were no(poster.facebookAdapter).publishStatusUpdate(any, any, any, any)
    }

    "reject subregions if there are no countries" in {
      val poster = new TestableFacebookPoster
      val params = new ActivityParameters(baseParams +("postType" -> "status update", "action" -> publishAction,
        "target" -> Json.parse(s"""{"countryCodes": [], "subregions": ["$ada"]}""")))

      try {
        poster.handleTask(params)
        failure("Expected an exception")
      } catch {
        case e: IllegalArgumentException => e.getMessage === "Exactly one country code is required when place names are used."
      }
      
      there were no(poster.facebookAdapter).publishStatusUpdate(any, any, any, any)
    }

    "resolve cities" in {
      val poster = new TestableFacebookPoster
      val params = new ActivityParameters(baseParams +("postType" -> "status update", "action" -> publishAction,
        "target" -> Json.parse(s"""{"countryCodes": ["$usa"], "cities": ["$boise","$meridian","NoSuchCity,Ada,ID"]}""")))
      poster.handleTask(params)
      
      val expectedTarget = new Target(List(usa), List(), List(boiseId, meridianId))
      there was one(poster.facebookAdapter).publishStatusUpdate(connectionMatcher, ===(pageId), ===(expectedTarget), ===(message))
     
      poster.result must beSome(statusUpdatePostId)
    }

    "reject cities if there are multiple countries" in {
      val poster = new TestableFacebookPoster
      val params = new ActivityParameters(baseParams +("postType" -> "status update", "action" -> publishAction,
        "target" -> Json.parse(s"""{"countryCodes": ["$usa","$canada"], "cities": ["$boise"]}""")))

      try {
        poster.handleTask(params)
        failure("Expected an exception")
      } catch {
        case e: IllegalArgumentException => e.getMessage === "Exactly one country code is required when place names are used."
      }
      
      there were no(poster.facebookAdapter).publishStatusUpdate(any, any, any, any)
    }

    "reject cities if there are no countries" in {
      val poster = new TestableFacebookPoster
      val params = new ActivityParameters(baseParams +("postType" -> "status update", "action" -> publishAction,
        "target" -> Json.parse(s"""{"countryCodes": [], "cities": ["$boise"]}""")))

      try {
        poster.handleTask(params)
        failure("Expected an exception")
      } catch {
        case e: IllegalArgumentException => e.getMessage === "Exactly one country code is required when place names are used."
      }

      there were no(poster.facebookAdapter).publishStatusUpdate(any, any, any, any)
    }

    // Stop the web server.
    step {
      server.tearDown()
      success
    }
  }
}

object TestFacebookPoster {
  val boiseId = 2436507
  val meridianId = 2437125
  val nampaId = 2437176

  /**
   * This class provides an HTTP server for testing.  It's defined in the companion object so the Jersey Test Framework
   * can instantiate it.
   */
  @Path("/")
  class MockWebServer extends JerseyTest {

    private val photoFileContents = "Pretend that this is a photograph"

    @GET
    @Path("photos/something.jpg")
    def getPhoto = photoFileContents

    @GET
    @Path("countries/{countryCode}/regions/{regionCode}/subregions/{subregion}/facebookcities")
    def getSubregionCities(@PathParam("countryCode") countryCode: String,
                           @PathParam("regionCode") regionCode: String,
                           @PathParam("subregion") subregion: String) = {
      (countryCode.toLowerCase, regionCode.toLowerCase, subregion.toLowerCase) match {
        case ("us", "id", "ada") => s"[$boiseId,$meridianId]"
        case ("us", "id", "canyon") => s"[$nampaId]"
        case _ => "[]"
      }
    }

    @GET
    @Path("countries/{countryCode}/regions/{regionCode}/subregions/{subregion}/facebookcities/{city}")
    def getSubregionCities(@PathParam("countryCode") countryCode: String,
                           @PathParam("regionCode") regionCode: String,
                           @PathParam("subregion") subregion: String,
                           @PathParam("city") city: String) = {
      (countryCode.toLowerCase, regionCode.toLowerCase, subregion.toLowerCase, city.toLowerCase) match {
        case ("us", "id", "ada", "boise") => boiseId
        case ("us", "id", "ada", "meridian") => meridianId
        case _ => 0
      }
    }

    override def configure = {
      forceSet(TestProperties.CONTAINER_PORT, "0") // Choose first available port.
      new ResourceConfig(getClass)
    }

    def getBaseUrl = getBaseUri.toURL

    def getPhotoFileContents = photoFileContents.getBytes
  }
}
