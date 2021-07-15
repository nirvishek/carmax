""" Main class for initialising and running crawlers
    This class is to manage information of any given input vehicle.
    Base class for running other crawlers (mmr, kbb, autoniq). """

import os
import logging
from scrapy import signals
from scrapy.exceptions import CloseSpider
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy.utils.project import get_project_settings
from bidding_portal_crawler.src.utils.parser import Parser
from bidding_portal_crawler.src.utils.read_config import ConfigReader


logger = logging.getLogger(__name__)


class VehicleInfoManager(object):

    """ Initialize crawler with project settings """
    def __init__(self, *spidercfg):
        settings_file_path = "bidding_portal_crawler.src.settings"
        os.environ.setdefault("SCRAPY_SETTINGS_MODULE", settings_file_path)
        print(os.environ["SCRAPY_SETTINGS_MODULE"])
        settings = get_project_settings()  # Scrapy settings
        self.process = CrawlerProcess(settings)
        print(self.process, )
        self.spidercfg = spidercfg

    """ Get all the sites to crawl from config file """
    def get_sites_to_run(self, scrape_site=None):

        logger.debug("VehicleInfoManager::get_sites_to_run::Getting sites to scrape from config file.")
        if scrape_site is None:
            section = "SCRAPE_SITE"
            try:
                config_obj = ConfigReader(section).read_config()
                sites_arr = config_obj['scrape_sites'].split(',')
            except KeyError as ke:
                logger.error("{} :: Section key {} not present in config.ini file.".format(ke, section))
                raise CloseSpider(reason="Error while getting site list from config.")

        else:
            sites_arr = scrape_site.split(",")

        logger.debug("VehicleInfoManager::get_sites_to_run:: Sites to scrape: %s" % sites_arr)

        site_cfg = {}

        # construct site_cfg dictionary: {site_name: site_cfg}
        for site in sites_arr:
            try:
                cfg = ConfigReader(site.upper()).read_config()
                site_cfg[site] = cfg
            except KeyError as ke:
                logger.error("VehicleInfoManager::get_sites_to_run::{}::"
                             "configuration for {} could not be found in config.ini".format(ke, site))
                raise CloseSpider

        return site_cfg

    """ Instantiate crawler for every site with respective configuration, start crawler engine """
    def run_spiders(self, cfg, data=None):

        logger.debug("VehicleInfoManager::run_spiders::Scraper setup for Data: %s with config object: %s" % (data, cfg))

        """ This list will store items (response with provider) returned by scraper """
        results = []

        """ This method will be triggered whenever an item is yielded by the crawler """
        def crawler_results(signal, sender, item, response, spider):
            logger.debug("Yielded response from %s: %s" % (spider, item))
            results.append(item)

        """ Dispatcher will establish a link between crawler and the provided method,
        the method will be triggered based on the type of signal """
        try:
            dispatcher.connect(crawler_results, signal=signals.item_passed)

        except Exception as exception:
            logger.error("VehicleInfoManager::run_spiders %s" % exception)
            raise CloseSpider(reason="Error in dispatcher connect %s" % exception)

        spider = cfg.get('name')

        self.process.crawl(spider, config=cfg, data=data)
        self.process.start()  # the script will block here until crawling is finished

        return results

    """ parse respective responses generated by crawler """
    def parse_responses(self, cfg, response_list):

        logger.debug("VehicleInfoManager::parse_responses::Parsing response list: %s" % response_list)
        dto_list = []

        """ response list will be a list of dictionaries, with provider and response as the keys """
        for responses in response_list:

            try:
                response = responses.get('response')
                provider = responses.get('provider')  # get provider for particular response e.g carfax
                vehicle = responses.get('vehicle')

            except KeyError as ke:
                logger.error("VehicleInfoManager::parse_responses Missing key (response, provider or vin) "
                             "in responses dict.")
                raise CloseSpider(reason="Missing response, provider or vin in item returned from scraper.")

            except Exception as exception:
                logger.error("VehicleInfoManager::parse_responses %s" % exception)
                raise CloseSpider(reason="%s" % exception)

            try:
                report = cfg.get(provider)  # get report section with provider to get search attribute dictionary
            except Exception as exception:
                logger.error("VehicleInfoManager::parse_responses %s" % exception)
                raise CloseSpider("%s" % exception)

            # initialize parser class
            parser = Parser(report, provider, vehicle)
            vehicle_dto_obj = parser.parse_data(response)  # get dto object from parser

            # check if a list is returned
            if isinstance(vehicle_dto_obj, list):
                dto_list += vehicle_dto_obj
            else:
                dto_list.append(vehicle_dto_obj)  # make a list of dto objects returned

        return dto_list


    class VehicleCrawler(VehicleInfoManager):

        def main(self, site_arr):

            for site, cfg in site_arr.items():
                """ setup logger for appropriate scraper """
                site = site.lower()  # AUTONIQ to autoniq
                # setup_logger(site)

                # get list of vehicle from IVV carmax api
                vehicle_list = requests.post(cfg.get("ivv_api_endpoint"), payload=None)
        
                # keep the vehicle list as a list of json
                # Start crawler for carmax:
                """ run crawler for every site and get response list """
                responses = self.run_spiders(cfg, data=vehicle_list)
                
                # 5. construct the payload with all vehicle details and hit saveBulk URL request to submit the request.

                """ pass response list to the parser """
                vehicle_dto_list = self.parse_responses(cfg, responses)


    def main(self, site_arr):

        scraper = VehicleCrawler()

        """ get site list and corresponding config into a dictionary """
        if len(sys.argv)==2:
            scrape_site = sys.argv[1]
            site_list = scraper.get_sites_to_run(scrape_site=scrape_site)
        else:
            site_list = scraper.get_sites_to_run()

        scraper.main(site_list)


