# pylint: disable=super-init-not-called


from typing import Optional

import simplejson
import singer
from requests import Response


LOGGER = singer.get_logger()


class TapSalesforceException(Exception):
    pass


class QueryLengthExceedLimit(Exception):
    pass


class TapSalesforceQuotaExceededException(TapSalesforceException):
    pass


class TapSalesforceOauthException(TapSalesforceException):
    pass


class TapSalesforceInvalidCredentialsException(TapSalesforceException):
    pass


class SalesforceException(Exception):
    def __init__(self, message: str, code: Optional[str] = None) -> None:
        super().__init__(message)
        self.code = code


# build_salesforce_exception transforms a generic Response into a SalesforceException if the
# response body has a salesforce exception, returns None otherwise
# salesforce error body looks like:
# [
#   {
#       'message': 'Your query request was running for too long.',
#       'errorCode': 'QUERY_TIMEOUT',
#   }
# ]
def build_salesforce_exception(resp: Response) -> Optional[SalesforceException]:
    try:
        err_array = resp.json()
    except simplejson.scanner.JSONDecodeError:
        LOGGER.error(f"Failed to parse response body: {resp.text}")
        return SalesforceException("response code: " + str(resp.status_code), "UNKNOWN")

    if not isinstance(err_array, list):
        return None

    if len(err_array) < 1:
        return None

    err_dict = err_array[0]

    if not isinstance(err_dict, dict):
        return None

    msg = err_dict.get("message")
    if msg is None:
        return None

    code = err_dict.get("errorCode")

    return SalesforceException(msg, code)
