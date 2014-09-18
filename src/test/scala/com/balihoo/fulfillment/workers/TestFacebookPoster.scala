package com.balihoo.fulfillment.workers

import java.net.URL
import javax.ws.rs._
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.amazonaws.services.simpleworkflow.model.ActivityTask
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import com.balihoo.socialmedia.facebook.FacebookConnection
import com.balihoo.socialmedia.facebook.model.Target
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.test.{TestProperties, JerseyTest}
import org.junit.runner.RunWith
import org.specs2.matcher.Matcher
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TestFacebookPoster extends Specification with Mockito {

  /**
   * A testable version of FacebookPoster with mocks
   * @param _facebookAdapter
   */
  class TestableFacebookPoster(_facebookAdapter: FacebookAdapter)
    extends AbstractFacebookPoster
    with LoggingWorkflowAdapter
    with FacebookAdapterComponent {

    def splog = mock[Splogger]
    def dynamoAdapter = mock[DynamoAdapter]
    def swfAdapter = {
      val _config = mock[PropertiesLoader]
      _config.getString(anyString) returns "mock"
      _config.getString("name") returns "workername"

      val _client = mock[AmazonSimpleWorkflowAsyncClient]

      val _swfAdapter = mock[SWFAdapter]
      _swfAdapter.domain returns "mockdomain"
      _swfAdapter.config returns _config
      _swfAdapter.client returns _client

      _swfAdapter
    }
    def facebookAdapter = _facebookAdapter
    task = mock[ActivityTask]
  }

  // Implicitly convert List[Int] to java.util.List[Integer]
  implicit def listOfIntToJavaListOfInteger(list: List[Int]): java.util.List[Integer] = list.map(Integer.valueOf(_))

  // Constants
  val appId = "439034opifpiojew4fpo34p9o4fok"
  val appSecret = "09340934fopijfpioj34f0t934poijwe5gpoijrfpokefa4wp9k34fvpoke4f"
  val accessToken = "98349834lkidflkj3q4f0934094flke0p9w4f4opij34f9j34f0934fmervlgkjergf5opji34op9j345io34fjio3498043to0we4rtsdrf"
  val pageId = "43984984380349034095905309354"
  val message = "What's up?"

  // Mock FacebookAdapter
  val facebookAdapter = mock[FacebookAdapter]

  // Expected FacebookConnection
  val connection = new FacebookConnection(appId, appSecret, accessToken)
  val connectionMatcher: Matcher[FacebookConnection] = (c: FacebookConnection) =>
    (appId.equals(c.getAppId) && appSecret.equals(c.getAppSecret) && accessToken.equals(c.getAccessToken), "Connection doesn't match")

  // Expected Target
  val target = new Target(List("US", "CA"), List(12, 34), List(56, 78, 90))

  // Parameters that all tests have in common
  val baseParams = Map(
    "appId" -> appId,
    "appSecret" -> appSecret,
    "accessToken" -> accessToken,
    "pageId" -> pageId,
    "target" -> "{\"countryCodes\": [\"US\",\"CA\"], \"regionIds\": [12,34], \"cityIds\": [56,78,90]}",
    "message" -> message)

  // Optional parameter values
  val linkUrl = "http://balihoo.com/test"
  val photoUrl = ""

  // Available actions
  val validateAction = "validate"
  val publishAction = "publish"

  // We'll need a web server to host a photo.
  val server = new MockPhotoServer

  "FacebookPoster" should {
    "validate a link post" in {
      val poster = new TestableFacebookPoster(facebookAdapter)
      val params = new ActivityParameters(baseParams + ("postType" -> "link", "linkUrl" -> linkUrl, "action" -> validateAction))
      val result = poster.handleTask(params)

      // Verify that the post was processed.
      there was one(facebookAdapter).validateLinkPost(connectionMatcher, ===(pageId), ===(target), ===(linkUrl), ===(message))

      // Verify that the task completed.
      poster.completedTasks mustEqual 1
    }

    "publish a link post" in {
      val poster = new TestableFacebookPoster(facebookAdapter)
      val params = new ActivityParameters(baseParams + ("postType" -> "link", "linkUrl" -> linkUrl, "action" -> publishAction))
      val result = poster.handleTask(params)

      // Verify that the post was processed.
      there was one(facebookAdapter).publishLinkPost(connectionMatcher, ===(pageId), ===(target), ===(linkUrl), ===(message))

      // Verify that the task completed.
      poster.completedTasks mustEqual 1
    }

    "validate a status update" in {
      val poster = new TestableFacebookPoster(facebookAdapter)
      val params = new ActivityParameters(baseParams + ("postType" -> "status update", "action" -> validateAction))
      val result = poster.handleTask(params)

      // Verify that the post was processed.
      there was one(facebookAdapter).validateStatusUpdate(connectionMatcher, ===(pageId), ===(target), ===(message))

      // Verify that the task completed.
      poster.completedTasks mustEqual 1
    }

    "publish a status update" in {
      val poster = new TestableFacebookPoster(facebookAdapter)
      val params = new ActivityParameters(baseParams + ("postType" -> "status update", "action" -> publishAction))
      val result = poster.handleTask(params)

      // Verify that the post was processed.
      there was one(facebookAdapter).publishStatusUpdate(connectionMatcher, ===(pageId), ===(target), ===(message))

      // Verify that the task completed.
      poster.completedTasks mustEqual 1
    }

    // Fire up the web server for the photo tests.
    step {
      server.setUp()
      success
    }

    "validate a photo post" in {
      val poster = new TestableFacebookPoster(facebookAdapter)
      val params = new ActivityParameters(baseParams + ("postType" -> "photo", "photoUrl" -> server.getPhotoUrl, "action" -> validateAction))
      val result = poster.handleTask(params)

      // Verify that the post was processed.
      there was one(facebookAdapter).validatePhotoPost(connectionMatcher, ===(pageId), ===(target), ===(server.photoFileContents), ===(server.photoName), ===(message))

      // Verify that the task completed.
      poster.completedTasks mustEqual 1
    }

    "publish a photo post" in {
      val poster = new TestableFacebookPoster(facebookAdapter)
      val params = new ActivityParameters(baseParams + ("postType" -> "photo", "photoUrl" -> server.getPhotoUrl, "action" -> publishAction))
      val result = poster.handleTask(params)

      // Verify that the post was processed.
      there was one(facebookAdapter).publishPhotoPost(connectionMatcher, ===(pageId), ===(target), ===(server.photoFileContents), ===(server.photoName), ===(message))

      // Verify that the task completed.
      poster.completedTasks mustEqual 1
    }

    // Stop the web server.
    step {
      server.tearDown()
      success
    }
  }

}

// This class provides an HTTP server for testing.
@Path("/")
class MockPhotoServer extends JerseyTest {

  val photoName = "airport.jpg"
  private final val photoPath = "vacation/photos/airport.jpg"
  val photoFileContents = "Pretend that this is a photograph".getBytes 

  @GET
  @Path(photoPath)
  def getPhoto = photoFileContents

  override def configure = {
    forceSet(TestProperties.CONTAINER_PORT, "0") // Choose first available port.
    new ResourceConfig(getClass)
  }

  def getPhotoUrl = new URL(getBaseUri.toURL, photoPath).toString
}
