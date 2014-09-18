package com.balihoo.fulfillment.workers

import java.net.URL
import com.balihoo.fulfillment.adapters.{FacebookAdapter, FacebookAdapterComponent, LoggingWorkflowAdapterImpl, LoggingWorkflowAdapter}
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import com.balihoo.socialmedia.facebook.FacebookConnection
import com.balihoo.socialmedia.facebook.model.Target
import org.apache.commons.io.{FilenameUtils, IOUtils}
import play.api.libs.json._
import scala.collection.JavaConversions._

/**
 * Typical workflow
 * ****************
 * 1. Use a FacebookPoster worker to validate the post data.  This should happen as soon as possible after the order is
 * created.
 *
 * 2. Use a FacebookPoster worker to publish the post at the desired time.
 */
abstract class AbstractFacebookPoster extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with FacebookAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ActivityParameter("appId", "string", "The Facebook app ID"),
      new ActivityParameter("appSecret", "string", "The Facebook app secret", sensitive = true),
      new ActivityParameter("accessToken", "string", "The Facebook access token", sensitive = true),
      new ActivityParameter("postType", "string", "\"link\", \"photo\", or \"status update\""),
      new ActivityParameter("pageId", "string", "The Facebook page ID"),
      new ActivityParameter("target", "JSON", "The targeting data"),
      new ActivityParameter("message", "string", "The message to post", required = false),
      new ActivityParameter("linkUrl", "string", "A link to include in the post", required = false),
      new ActivityParameter("photoUrl", "string", "The URL of the photo to include in the post", required = false),
      new ActivityParameter("action", "string", "\"validate\" or \"publish\"")
    ), new ActivityResult("string", "the Facebook post ID if the action is \"publish\", otherwise ignore this value"))
  }

  /**
   * Gets the filename of an optional URL.
   * @param url The URL
   * @return The name of the file referenced by the URL, or null if there's no URL
   */
  private def getPhotoName(url: Option[String]): String = {
    url match {
      case Some(u) => FilenameUtils.getName(u)
      case None => null
    }
  }

  /**
   * Downloads an optional photo and turns it into a byte array.
   * @param url The photo's URL
   * @return the file contents, or null if there's no URL
   */
  private def getPhotoBytes(url: Option[String]): Array[Byte] = {
    url match {
      case Some(u) => {
        splog.info(s"Facebook poster downloading $u")
        IOUtils.toByteArray(new URL(u))
      }
      case None => null
    }
  }

  override def handleTask(params: ActivityParameters) = {
    println(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    withTaskHandling {
      val appId = params("appId")
      val appSecret = params("appSecret")
      val accessToken = params("accessToken")
      val connection = new FacebookConnection(appId, appSecret, accessToken)
      val postType = params("postType")
      val pageId = params("pageId")
      val target = createTarget(params("target"))
      val message = params.getOrElse("message", null)
      lazy val linkUrl = params.getOrElse("linkUrl", null)
      lazy val photoUrl = params.get("photoUrl")
      lazy val photoBytes = getPhotoBytes(photoUrl)
      lazy val photoName = getPhotoName(photoUrl)
      val action = params("action")
      splog.info(s"Facebook poster was asked to $action a $postType. The page ID is $pageId.")

      (action, postType) match {
        case ("validate", "link") => facebookAdapter.validateLinkPost(connection, pageId, target, linkUrl, message); "OK"
        case ("validate", "photo") => facebookAdapter.validatePhotoPost(connection, pageId, target, photoBytes, photoName, message); "OK"
        case ("validate", "status update") => facebookAdapter.validateStatusUpdate(connection, pageId, target, message); "OK"
        case ("publish", "link") => facebookAdapter.publishLinkPost(connection, pageId, target, linkUrl, message)
        case ("publish", "photo") => facebookAdapter.publishPhotoPost(connection, pageId, target, photoBytes, photoName, message)
        case ("publish", "status update") => facebookAdapter.publishStatusUpdate(connection, pageId, target, message)
        case _ => throw new IllegalArgumentException(s"Invalid action or post type: $action $postType")
      }
    }
  }

  /**
   * Converts a JSON string into a Target object.
   * @param jsonString the input
   * @return the output
   */
  private def createTarget(jsonString: String): Target = {
    val json = Json.parse(jsonString).as[JsObject]

    val countryCodes = (json \ "countryCodes").as[Seq[String]]
    val regionIds = (json \ "regionIds").as[Seq[Int]].map(Integer.valueOf(_))
    val cityIds = (json \ "cityIds").as[Seq[Int]].map(Integer.valueOf(_))

    new Target(countryCodes, regionIds, cityIds)
  }
}

class FacebookPoster(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractFacebookPoster
  with LoggingWorkflowAdapterImpl
  with FacebookAdapterComponent {
    lazy private val _facebookAdapter = new FacebookAdapter
    def facebookAdapter = _facebookAdapter
}

object facebook_poster extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new FacebookPoster(cfg, splog)
  }
}
