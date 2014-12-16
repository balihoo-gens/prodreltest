package com.balihoo.fulfillment.workers.test

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.workers.adwords.{ImageAdCreatorComponent, CampaignCreatorComponent, AdGroupCreatorComponent}

abstract class ImageAdTest(cfg: PropertiesLoader)
    extends AdWordsAdapterComponent
      with CampaignCreatorComponent
      with AdGroupCreatorComponent
      with ImageAdCreatorComponent {
    private val _awa = new AdWordsAdapter(cfg)
    def adWordsAdapter = _awa
    private val _cc = new CampaignCreator(adWordsAdapter)
    private val _ac = new AdGroupCreator(adWordsAdapter)
    private val _adc = new AdCreator(adWordsAdapter)
    def campaignCreator = _cc
    def adGroupCreator = _ac
    def adCreator = _adc

    def run: Unit
}

object adWordsGetAdGroupImageAd {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_imageadprocessor")
    val test = new TestGetAdGroupImageAd(cfg)
    test.run
  }

  class TestGetAdGroupImageAd(cfg: PropertiesLoader) extends ImageAdTest(cfg) {
    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687") //  Balihoo > Balihoo Dogtopia | fulfillment test ( Client ID: 100-019-2687 )

      val campaignParams = new ActivityArgs(Map(
         "name" -> "fulfillment Campaign",
          "channel" -> "DISPLAY"
      ))
      val campaign = campaignCreator.getCampaign(campaignParams)
      val adgroupParams = new ActivityArgs(Map(
          "name" -> "GROUP A",
          "campaignId" -> s"${campaign.getId}"
      ))
      val adgroup = adGroupCreator.getAdGroup(adgroupParams)

      val imageAdParams = new ActivityArgs(Map(
         "name" -> "Another Nature",
          "adGroupId" -> s"${adgroup.getId}"
      ))
      val ad = adCreator.getImageAd(imageAdParams)

      println(ad.toString)
    }
  }
}

object adWordsAdGroupImageAd {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_imageadprocessor")
    val test = new TestAdGroupImageAd(cfg)
    test.run
  }

  class TestAdGroupImageAd(cfg: PropertiesLoader) extends ImageAdTest(cfg) {
    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687") //  Balihoo > Balihoo Dogtopia | fulfillment test ( Client ID: 100-019-2687 )


      val campaignParams = Map(
         "name" -> "fulfillment Campaign",
          "channel" -> "DISPLAY"
      )
      val campaign = campaignCreator.getCampaign(new ActivityArgs(campaignParams))
      val adgroupParams = Map(
         "name" -> "GROUP A",
          "campaignId" -> s"${campaign.getId}"
      )
      val adgroup = adGroupCreator.getAdGroup(new ActivityArgs(adgroupParams))

      val imageAdParams = Map(
         "name" -> "Test 5",
          "adGroupId" -> s"${adgroup.getId}",
          "url" -> "http://balihoo.com",
          "displayUrl" ->    "http://balihoo.com",
          "imageUrl" -> "http://placehold.it/200x200"
      )

      val imageAd = adCreator.createImageAd(new ActivityArgs(imageAdParams))
      println(imageAd.getName)

    }
  }
}

object adWordsUpdateAdGroupImageAd {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_imageadprocessor")
    val test = new TestUpdateAdGroupImageAd(cfg)
    test.run
  }

  class TestUpdateAdGroupImageAd(cfg: PropertiesLoader) extends ImageAdTest(cfg) {
    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687") //  Balihoo > Balihoo Dogtopia | fulfillment test ( Client ID: 100-019-2687 )

      val campaignParams = Map(
         "name" -> "fulfillment Campaign",
          "channel" -> "DISPLAY"
        )
      val campaign = campaignCreator.getCampaign(new ActivityArgs(campaignParams))
      val adgroupParams = Map(
         "name" -> "GROUP A",
          "campaignId" -> s"${campaign.getId}"
        )

      val adgroup = adGroupCreator.getAdGroup(new ActivityArgs(adgroupParams))

      val imageAdParams = Map(
         "name" -> "Drab",
          "adGroupId" -> s"${adgroup.getId}",
          "url" -> "http://balihoo.com",
          "displayUrl" ->    "http://balihoo.com/stuff",
//          "imageUrl" -> "http://lorempixel.com/300/100/nature/"
            "imageUrl" -> "http://dummyimage.com/200x200/000/1f1"
        )

      val ad = adCreator.getImageAd(new ActivityArgs(imageAdParams))

      val imageAd = adCreator.updateImageAd(ad, new ActivityArgs(imageAdParams))
      println(imageAd.getName)
    }
  }
}

/*
object adWordsLookupMedia {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_imageadprocessor")
    val test = new TestLookupMedia(cfg)
    test.run
  }

  class TestLookupMedia(cfg: PropertiesLoader) extends ImageAdTest(cfg) {
    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687") //  Balihoo > Balihoo Dogtopia | fulfillment test ( Client ID: 100-019-2687 )

      val ad = adCreator.lookupMedia("D1C938E03C7D5A448C9FFA8619875E5B")

    }
  }
}
  */

     /*
     * This method DOES NOT WORK. We can't query images we've uploaded by name because
     * ADWORDS doesn't actually populate the Name field! This is a major bummer!
     *
    def lookupMedia(name:String):Media = {

      val context = s"lookupMedia(name='$name')"

      val selector = new SelectorBuilder()
        .fields("Name", "MediaId", "FileSize", "Dimensions", "SourceUrl", "ReferenceId")
        .equals("Name", name)
        .equals("Type", "IMAGE")
        .build()

      adWordsAdapter.withErrorsHandled[Media](context, {
        val page = adWordsAdapter.mediaService.get(selector)
        println(s"There are ${page.getTotalNumEntries} entries!")
        page.getTotalNumEntries.intValue() match {
          case 0 => null
          case _ =>
            for(item:Media <- page.getEntries) {
              println(s" -- ${item.getName} ${item.getSourceUrl} ${item.getFileSize} ${item.getMediaId} ${item.getReferenceId}")
            }
            page.getEntries(0)
//          case _ => throw new Exception(s"sourceUrl $name is ambiguous!")
        }
      })
    } */

    /*
    val uploadedImage = adWordsAdapter.withErrorsHandled[Array[Media]]("Uploading new image", {
      adWordsAdapter.mediaService.upload(Array(image))
    })(0).asInstanceOf[Image]

    image.setMediaId(uploadedImage.getMediaId)
    */