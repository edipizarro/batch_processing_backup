from batch_processing.ztf import ZTFCrawler
from performance_timer import PerformanceTimer

with PerformanceTimer("[SETUP] initialize ZTFCrawler"):
    config_path = "/home/user/projects/batch_processing/batch_processing/configs/raw.config.json"
    ztf = ZTFCrawler(config_path)

with PerformanceTimer("[ZTFCrawler] execute"):
    ztf.execute()
