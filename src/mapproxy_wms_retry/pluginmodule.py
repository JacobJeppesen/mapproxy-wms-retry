import logging
import time

from mapproxy.client.http import HTTPClient
from mapproxy.config.loader import WMSSourceConfiguration, register_source_configuration
from mapproxy.config.spec import mapproxy_yaml_spec
from mapproxy.util.ext.dictspec.spec import required

SPEC = list(mapproxy_yaml_spec["sources"].values())[0].specs["wms"]
SPEC["retry"] = {
    required("max_retries"): int(),
    required("retry_delay"): int(),  # delay in seconds
}

logger = logging.getLogger("mapproxy.wms_retry")


class HTTPClientRetry(HTTPClient):
    def __init__(self, max_retries, retry_delay):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        super().__init__()

    def open(self, *args, **kwargs):
        for i in range(self.max_retries):
            try:
                result = super().open(*args, **kwargs)
                # Check if status code is not successful (200-299)
                if hasattr(result, "status_code") and (
                    result.status_code < 200 or result.status_code >= 300
                ):
                    logger.warning(
                        f"HTTP status {result.status_code} received, retrying in {self.retry_delay}s ({i+1}/{self.max_retries})"
                    )
                    time.sleep(self.retry_delay)
                    continue
                return result
            except Exception as e:
                # Also catch connection errors and other exceptions
                logger.warning(
                    f"Request failed with error: {e}, retrying in {self.retry_delay}s ({i+1}/{self.max_retries})"
                )
                time.sleep(self.retry_delay)
                continue

        # If we've exhausted all retries, try one last time
        return super().open(*args, **kwargs)


class wms_retry_configuration(WMSSourceConfiguration):
    source_type = ("wms_retry",)

    def source(self, params=None):
        # Custom parameters
        retry = self.conf["retry"]
        retry_params = {
            "max_retries": retry.get("max_retries", 3),
            "retry_delay": retry.get("retry_delay", 1),
        }

        # Create WMS source
        wmssource = super().source(params)

        # Replace HTTP client
        wmssource.client.http_client = HTTPClientRetry(**retry_params)

        return wmssource


def plugin_entrypoint():
    register_source_configuration(
        "wms_retry", wms_retry_configuration, "wms_retry", SPEC
    )
