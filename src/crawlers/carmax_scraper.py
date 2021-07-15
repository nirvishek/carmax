# -*- coding: utf-8 -*-
""" Scraper class for getting vehicles from Carmax portal """
import scrapy
import json
import logging
from src.crawler.scraper import Scraper
from scrapy.utils.project import get_project_settings
import requests

logger = logging.getLogger("root")


class CarmaxScraper(Scraper):
    name = 'carmax_spider'
    allowed_domains = ['carmax.com']

    # Allow duplicate url request (we will be crawling "page 1" twice)
    # custom_settings will only apply these settings in this spider
    custom_settings = {
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
        'ROBOTSTXT_OBEY': False
    }

    def __init__(self, config=None, data=None):
        super(CarmaxScraper, self).__init__()
        self.cfg = config
        self.login_url = self.cfg.get('login_url')
        self.start_urls = [self.login_url]
        self.vehicle_list = data

    def start_requests(self):
        """ inbuilt start method called by scrapy when initializing crawler. """
        logger.debug("Carmax::start_requests Starting Carmax crawler: %s." % self.start_urls)

        for url in self.start_urls:
            yield scrapy.Request(url,
                                 callback=self.bulk_appraisal)

    def bulk_appraisal(self, response):
        yield scrapy.Request(self.cfg.get("bulk_appraisal_url"),
                                callback=self.get_token)

    def decode_vin(self, response):
        """ method to extract token from login page, construct login url,
        and calling base login method with the url """

        logger.debug("CarmaxCrawler::get_token Getting token for login url before login: %s" % response)

        # get form bulk appraisal token
        token = str(response.css('form input::attr(value)').extract_first())
        logger.debug("CarmaxScraper::get_token Token: %s" % token)

        # calling base class login
        # response = self.login(token, self.login_url, self.is_authenticated, self.cfg)
        for vehicle in self.vehicle_list:
            yield response.Request(decode_vin_url.format(vehicle.get("vin")),
                                    callback=yield_item,
                                    meta={
                                        "token": token
                                    }
                                )

    def yield_item(self, response):

        """ yield response back to dispatcher method in main class. """
        item = {
            'response': response,
            'appraisal_token': response.meta["token"],
        }

        logger.debug(item)
        # logger.debug(json.loads(response.meta['summary']))

        yield item
