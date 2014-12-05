package com.balihoo.fulfillment.workers

import java.io.{ByteArrayInputStream, InputStreamReader}
import java.net.URI
import com.amazonaws.AmazonServiceException
import com.balihoo.fulfillment.adapters._
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Scope
import play.api.libs.json.Json
import scala.io.Codec

@RunWith(classOf[JUnitRunner])
class TestSendGridEmail extends Specification with Mockito {
  "SendGridEmail" should {
    "handle a normal request" in new Adapter {
      worker.handleTask(params)
      worker.result.get === "OK"
      there was one(_sendGridAdapter).sendEmail(credentials, uniqueArgs, sendTime, email, recipientsStream, recipientIdHeading, emailHeading)
    }

    "reject a bad list URL" in new Adapter {
      val params2 = new ActivityParameters(params.params + ("listUrl" -> bogusUrl))
      worker.handleTask(params2) must throwA[SendGridException]
    }

    "reject a bad email body URL" in new Adapter {
      val params2 = new ActivityParameters(params.params + ("bodyUrl" -> bogusUrl))
      worker.handleTask(params2) must throwA[SendGridException]
    }
  }

  trait Adapter extends Scope {
    val emailBody = "<html>Hello, %%name%%! I heard you like %%likes%%.</html>"
    val recipients = """
                       |personId,name,"email",likes
                       |17,User1,user1@balihoo.com,fish
                       |"22","user2","user2@balihoo.com","tacos, with ""cheese"" "
                     """.stripMargin.trim
    val bucket = "chumBucket"
    val bodyKey = "body983983"
    val listKey = "list9398844"
    val bogusKey = "bogus"
    val bogusUrl = new URI(s"S3://$bucket/$bogusKey")
    val recipientsStream = Stream(
      List("personId", "name", "email", "likes"),
      List("17", "User1", "user1@balihoo.com", "fish"),
      List("22", "user2", "user2@balihoo.com", "tacos, with \"cheese\" "))
    val uniqueArgs = Json.obj("ocean" -> "Pacific", "fish" -> "salmon")
    val subaccount = "TestAccount17"
    val email = Email(fromAddress = "spammer@spam.them.all", fromName = "Not a spammer", replyToAddress = "noreply@go.fish",
      subject = "Best email ever!!!", body = emailBody)
    val credentials = SendGridCredentials(subaccount, "pword")
    val sendTime = new DateTime("2014-12-02T18:00:00-07")
    val recipientIdHeading = "personId"
    val emailHeading = "email"
    val params = new ActivityParameters(Map(
      "uniqueArgs" -> uniqueArgs,
      "subaccount" -> subaccount,
      "listUrl" -> new URI(s"S3://$bucket/$listKey"),
      "subject" -> email.subject,
      "fromAddress" -> email.fromAddress,
      "fromName" -> email.fromName,
      "replyToAddress" -> email.replyToAddress,
      "bodyUrl" -> new URI(s"S3://$bucket/$bodyKey"),
      "sendTime" -> sendTime,
      "recipientIdHeading" -> recipientIdHeading,
      "emailHeading" -> emailHeading))

    val _sendGridAdapter = mock[SendGridAdapter]
    _sendGridAdapter.getCredentials(subaccount) returns credentials

    val _s3Adapter = mock[S3Adapter]
    _s3Adapter.getObjectContentAsString(===(bucket), ===(bodyKey))(any[Codec]) returns emailBody
    _s3Adapter.getObjectContentAsReader(===(bucket), ===(listKey), anyString) returns new InputStreamReader(new ByteArrayInputStream(recipients.getBytes))
    _s3Adapter.getObjectContentAsString(===(bucket), ===(bogusKey))(any[Codec]) throws new AmazonServiceException("Bogus!")

    val worker = new AbstractSendGridEmail
      with LoggingWorkflowAdapterTestImpl
      with SendGridAdapterComponent
      with ScalaCsvAdapterComponent
      with S3AdapterComponent {

      def sendGridAdapter = _sendGridAdapter

      // Storage place for the task result
      var result: Option[String] = None

      override def withTaskHandling(code: => String): Unit = result = Some(code)

      /**
       * The call to Stream.toList causes the Stream object to buffer all its content.  This allows the Specs2 argument
       * matcher to verify the content after the stream is closed.
       */
      override def processStream(s: Stream[Any]): Unit = s.toList

      override val s3Adapter = _s3Adapter
    }
  }
}
