# pylint: disable=super-init-not-called
import json
from typing import Optional, List, Tuple

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


class TapSalesforceMissingTablesException(TapSalesforceException):
    tables : List[str] = []

    def __init__(self, tables: List[str]) -> None:
        self.tables = tables
        super().__init__(self.__str__())

    def __str__(self) -> str:
        return "Account is missing access to objects: " + ", ".join(self.tables)


class TapSalesforceReportException(TapSalesforceException):
    exceptions : List[Exception] = []

    def __init__(self, *exceptions: Exception) -> None:
        self.exceptions = list(exceptions)
        super().__init__(self.__str__())

    def __str__(self) -> str:
        return json.dumps([{"type": type(e).__name__, "exception": str(e)} for e in self.exceptions])

    def add(self, exception: Exception) -> None:
        self.exceptions.append(exception)


class SalesforceException(Exception):
    def __init__(self, message: str, code: Optional[str] = None) -> None:
        super().__init__(message)
        self.code = code

    def __str__(self) -> str:
        return f"{self.code}: {super().__str__()}"


class SalesforceFunctionalityTemporarilyUnavailableException(SalesforceException):
    def __init__(self, message: str) -> None:
        super().__init__(message, "FUNCTIONALITY_TEMPORARILY_UNAVAILABLE")


class SalesforceAPIDisabledForOrganizationException(SalesforceException):
    def __init__(self, message: str) -> None:
        super().__init__(message, "API_DISABLED_FOR_ORG")


class SalesforceSessionExpiredException(SalesforceException):
    def __init__(self, message: str) -> None:
        super().__init__(message, "SESSION_EXPIRED")


class SalesforceUnexpectedException(SalesforceException):
    def __init__(self, message: str) -> None:
        super().__init__(message, "UNEXPECTED")


class SalesforceQueryTimeoutException(SalesforceException):
    def __init__(self, message: str) -> None:
        super().__init__(message, "QUERY_TIMEOUT")


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

    if code == "FUNCTIONALITY_TEMPORARILY_UNAVAILABLE":
        return SalesforceFunctionalityTemporarilyUnavailableException(msg)

    if code == "API_DISABLED_FOR_ORG":
        return SalesforceAPIDisabledForOrganizationException(msg)

    if "Session expired or invalid" in msg:
        return SalesforceSessionExpiredException(msg)

    if "An unexpected error occurred" in msg:
        return SalesforceUnexpectedException(msg)

    if "Your query request was running for too long" in msg:
        return SalesforceQueryTimeoutException(msg)

    return SalesforceException(msg, code)
