""" Base scraper class for all scrapers """

import scrapy
import json
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError
from scrapy.exceptions import CloseSpider


class Scraper(scrapy.Spider):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    """ scrapy starts crawling by calling this method """
    def start_requests(self):
        pass

    """ method to extract or parse data required to login """
    def prepare_login_request(self, response):
        pass

    """ generic login request method """
    def login(self, token, url, callback_method, config):

        self.logger.debug("Scraper::login::Authenticating with login url: %s" % url)

        tokens = {val.split(',')[0]: val.split(',')[1] for key, val in config.items() if "token" in key}

        form_data = {
            config['username_field']: config['username'],
            config['password_field']: config['password'],
        }

        # merge credentials dict with token dict
        form_data.update(tokens)

        self.logger.debug("Scraper::login::Form data: %s" % form_data)
        return scrapy.FormRequest(
            url,
            formdata=form_data,
            callback=callback_method,
            errback=self.errback_handler
        )

    """ Callback Handler for different errors occurred while making requests """
    def errback_handler(self, failure):
        self.logger.error(repr(failure))

        # if isinstance(failure.value, HttpError):
        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)

        # elif isinstance(failure.value, DNSLookupError):
        elif failure.check(DNSLookupError):
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)

        # elif isinstance(failure.value, TimeoutError):
        elif failure.check(TimeoutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)

        raise CloseSpider

    """ method to check if login successful or not, can be overridden in derived class if 
    required for that crawler """
    def is_authenticated(self, response):

        response_dict = json.loads(response.text)

        if response_dict["success"]:
            self.logger.info("Scraper::is_authenticated::Login successful.")
            return self.navigate_to(response)
        else:
            self.logger.error("Scraper::is_authenticated::Login failed.")
            raise CloseSpider(reason="Login failed.")

    """ Once authenticated, request goes into this method for navigating to required pages """
    def navigate_to(self, response):
        pass

    """ Method for crawling a page once navigation done """
    def parser(self, response):
        pass

    """ method to write response data into a local file"""
    def write_data(self, filename, data, ftype):
        with open(filename, 'w') as f:
            if ftype == 'json':
                json.dump(data, f, indent=2)
            elif ftype == 'text':
                f.write(data)
            else:
                print("Type not recognized.")
