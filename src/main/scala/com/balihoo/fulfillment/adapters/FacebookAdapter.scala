package com.balihoo.fulfillment.adapters

import com.balihoo.socialmedia.facebook.{FacebookConnection, BalihooFacebookClient}
import com.balihoo.socialmedia.facebook.model.Target
import com.balihoo.socialmedia.facebook.post._

trait FacebookAdapterComponent {
  def facebookAdapter: AbstractFacebookAdapter
}

abstract class AbstractFacebookAdapter {
  /**
   * Validates the data for a link post.  If the post will be delivered in the future, call this once at the
   * beginning of the workflow to verify that we have all the data we'll need later.  An exception will be thrown if
   * anything's missing.
   * @param pageId the Facebook page ID
   * @param target the post targeting data
   * @param linkUrl the link to include in the post
   * @param message the message to post
   */
  def validateLinkPost(connection: FacebookConnection, pageId: String, target: Target, linkUrl: String, message: String): Unit

  /**
   * Publishes a link post.
   * @param pageId the Facebook page ID
   * @param target the post targeting data
   * @param linkUrl the link to include in the post
   * @param message the message to post
   * @return the post ID
   */
  def publishLinkPost(connection: FacebookConnection, pageId: String, target: Target, linkUrl: String, message: String): String

  /**
   * Validates the data for a photo post.  If the post will be delivered in the future, call this once at the
   * beginning of the workflow to verify that we have all the data we'll need later.  An exception will be thrown if
   * anything's missing.
   * @param pageId the Facebook page ID
   * @param target the post targeting data
   * @param photo a byte array containing the photo file contents
   * @param filename the photo's filename
   * @param message the message to post
   */
  def validatePhotoPost(connection: FacebookConnection, pageId: String, target: Target, photo: Array[Byte], filename: String, message: String): Unit

  /**
   * Publishes a photo post.
   * @param pageId the Facebook page ID
   * @param target the post targeting data
   * @param photo a byte array containing the photo file contents
   * @param filename the photo's filename
   * @param message the message to post
   * @return the post ID
   */
  def publishPhotoPost(connection: FacebookConnection, pageId: String, target: Target, photo: Array[Byte], filename: String, message: String): String

  /**
   * Validates the data for a status update.  If the post will be delivered in the future, call this once at the
   * beginning of the workflow to verify that we have all the data we'll need later.  An exception will be thrown if
   * anything's missing.
   * @param pageId the Facebook page ID
   * @param target the post targeting data
   * @param message the message to post
   */
  def validateStatusUpdate(connection: FacebookConnection, pageId: String, target: Target, message: String): Unit

  /**
   * Publishes a status update.
   * @param pageId the Facebook page ID
   * @param target the post targeting data
   * @param message the message to post
   * @return the post ID
   */
  def publishStatusUpdate(connection: FacebookConnection, pageId: String, target: Target, message: String): String
}

class FacebookAdapter() extends AbstractFacebookAdapter {
  // Define an implicit conversion from FacebookConnection to BalihooFacebookClient
  private implicit def connectionToClient(connection: FacebookConnection) = new BalihooFacebookClient(connection)

  override def validateLinkPost(connection: FacebookConnection, pageId: String, target: Target, linkUrl: String, message: String): Unit =
    new LinkPost(connection, pageId, target, linkUrl, message)

  override def publishLinkPost(connection: FacebookConnection, pageId: String, target: Target, linkUrl: String, message: String): String =
    publish(new LinkPost(connection, pageId, target, linkUrl, message))

  override def validatePhotoPost(connection: FacebookConnection, pageId: String, target: Target, photo: Array[Byte], filename: String, message: String): Unit =
    new PhotoPost(connection, pageId, target, photo, filename, message)

  override def publishPhotoPost(connection: FacebookConnection, pageId: String, target: Target, photo: Array[Byte], filename: String, message: String): String =
    publish(new PhotoPost(connection, pageId, target, photo, filename, message))

  override def validateStatusUpdate(connection: FacebookConnection, pageId: String, target: Target, message: String): Unit =
    new StatusUpdatePost(connection, pageId, target, message)

  override def publishStatusUpdate(connection: FacebookConnection, pageId: String, target: Target, message: String): String =
    publish(new StatusUpdatePost(connection, pageId, target, message))

  /**
   * Publishes a post while throttling API usage.
   * @param post the post to publish
   * @return the Facebook post ID
   */
  private def publish(post: FacebookPost): String = {
    // Facebook will cut us off if we exceed their API usage limit.  The exact limit is unpublished, but it's widely
    // believed that a rate of one call per second is safe.  The simplest way to throttle the workers is to add a small
    // delay here.  I'm going with two seconds in case we have two workers running.
    Thread.sleep(2000)
    post.publish();
  }
}
