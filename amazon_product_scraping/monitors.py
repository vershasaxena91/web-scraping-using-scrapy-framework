from spidermon import Monitor, MonitorSuite, monitors
from spidermon.contrib.scrapy.monitors import ItemCountMonitor


@monitors.name("Item count")
class ItemCountMonitor(Monitor):
    """
    A class method used to check whether at least 10 items were returned at the end of the spider execution.
    """

    @monitors.name("Minimum number of items")
    def test_minimum_number_of_items(self):
        """
        Definition of the monitor
        """

        item_extracted = getattr(self.data.stats, "item_scraped_count", 0)
        minimum_threshold = 10

        msg = "Extracted less than {} items".format(minimum_threshold)
        self.assertTrue(item_extracted >= minimum_threshold, msg=msg)


class SpiderCloseMonitorSuite(MonitorSuite):
    """
    Configuration of the monitor.
    """

    monitors = [ItemCountMonitor]
