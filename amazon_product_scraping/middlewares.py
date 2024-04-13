# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html


from random import choice
from scrapy import signals
from scrapy.exceptions import NotConfigured


class RotateUserAgentMiddleware(object):
    """
    A class used to rotate user-agent for each request.

    Attributes
    ----------
    user_agents : str
        user-agent
    """

    def __init__(self, user_agents):
        self.enabled = False
        self.user_agents = user_agents

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used by Scrapy to create a user-agent.

        Parameters
        ----------
        crawler : object
            crawler that uses this middleware

        Returns
        -------
        str
            user-agent
        """

        user_agents = crawler.settings.get("USER_AGENT", [])

        if not user_agents:
            raise NotConfigured("USER_AGENT not set or empty")

        user_agents = cls(user_agents)
        crawler.signals.connect(user_agents.spider_opened, signal=signals.spider_opened)

        return user_agents

    def spider_opened(self, spider):
        """
        A class method used to reserve per-spider resources, but can be used for any task that needs to be performed when a spider is opened.

        Parameters
        ----------
        spider : object
            the spider which has been opened
        """

        self.enabled = getattr(spider, "rotate_user_agent", self.enabled)

    def process_request(self, request, spider):
        """
        A class method is called for each request that goes through the download middleware.

        Parameters
        ----------
        request : object
            the request being processed
        spider : object
            the spider for which this request is intended

        Returns
        -------
        should either
            return None, return a Response object, return a Request object, or raise IgnoreRequest
        """

        if not self.enabled or not self.user_agents:
            return

        request.headers["user-agent"] = choice(self.user_agents)
