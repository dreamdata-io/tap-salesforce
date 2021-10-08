from typing import Optional, Tuple, Generator, Dict
from datetime import datetime, timedelta
import re
import backoff
from pydantic.main import BaseModel


import singer
import requests

from tap_salesforce.exceptions import (
    TapSalesforceOauthException,
    TapSalesforceQuotaExceededException,
)

QUERY_RESTRICTED_SALESFORCE_OBJECTS = set(
    [
        "Announcement",
        "ContentDocumentLink",
        "CollaborationGroupRecord",
        "Vote",
        "IdeaComment",
        "FieldDefinition",
        "PlatformAction",
        "UserEntityAccess",
        "RelationshipInfo",
        "ContentFolderMember",
        "ContentFolderItem",
        "SearchLayout",
        "SiteDetail",
        "EntityParticle",
        "OwnerChangeOptionInfo",
        "DataStatistics",
        "UserFieldAccess",
        "PicklistValueInfo",
        "RelationshipDomain",
        "FlexQueueItem",
    ]
)

QUERY_INCOMPATIBLE_SALESFORCE_OBJECTS = set(
    [
        "ListViewChartInstance",
        "FeedLike",
        "OutgoingEmail",
        "OutgoingEmailRelation",
        "FeedSignal",
        "ActivityHistory",
        "EmailStatus",
        "UserRecordAccess",
        "Name",
        "AggregateResult",
        "OpenActivity",
        "ProcessInstanceHistory",
        "OwnedContentDocument",
        "FolderedContentDocument",
        "FeedTrackedChange",
        "CombinedAttachment",
        "AttachedContentDocument",
        "ContentBody",
        "NoteAndAttachment",
        "LookedUpFromActivity",
        "AttachedContentNote",
        "QuoteTemplateRichTextData",
    ]
)

LOGGER = singer.get_logger()


def log_backoff_attempt(details):
    LOGGER.info(
        "ConnectionError detected, triggering backoff: %d try", details.get("tries")
    )


class Field(BaseModel):
    name: str
    type: str
    nullable: bool


class Salesforce:
    client_id: str
    client_secret: str
    session: requests.Session

    _access_token: Optional[str] = None
    _instance_url: Optional[str] = None
    _token_expiration_time: Optional[datetime] = None
    _metrics_http_requests: int = 0

    # CONSTANTS
    _REFRESH_TOKEN_EXPIRATION_PERIOD = 900
    _API_VERSION = "v52.0"
    _BLACKLISTED_FIELDS = QUERY_RESTRICTED_SALESFORCE_OBJECTS.union(
        QUERY_INCOMPATIBLE_SALESFORCE_OBJECTS
    )

    def __init__(
        self,
        refresh_token,
        client_id,
        client_secret,
        quota_percent_total: float = 80.0,
        quota_percent_per_run: float = 25.0,
        is_sandbox: bool = False,
    ):
        self.refresh_token = refresh_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.is_sandbox = is_sandbox

        self.quota_percent_total = quota_percent_total
        self.quota_percent_per_run = quota_percent_per_run

        self.session = requests.Session()
        self._login()

    def get_tables(self) -> Generator[Tuple[str, Dict[str, Field], str], None, None]:
        """returns the supported table names, as well as the replication_key"""
        tables = [
            ("Account", "LastModifiedDate"),
            ("Contact", "LastModifiedDate"),
            ("ContactHistory", "CreatedDate"),
            ("Lead", "LastModifiedDate"),
            ("Opportunity", "LastModifiedDate"),
            ("Campaign", "LastModifiedDate"),
            ("AccountContactRelation", "LastModifiedDate"),
            ("AccountContactRole", "LastModifiedDate"),
            ("OpportunityContactRole", "LastModifiedDate"),
            ("CampaignMember", "LastModifiedDate"),
            ("OpportunityHistory", "CreatedDate"),
            ("AccountHistory", "CreatedDate"),
            ("LeadHistory", "CreatedDate"),
            ("User", "LastModifiedDate"),
            ("Invoice__c", "LastModifiedDate"),
            ("Trial__c", "LastModifiedDate"),
            ("Task", "LastModifiedDate"),
            ("Event", "LastModifiedDate"),
        ]
        table: str
        replication_key: str
        for table, replication_key in tables:
            fields = self.get_fields(table)

            yield (table, fields, replication_key)

    def get_fields(self, table: str) -> Dict[str, Field]:
        """returns a list of all fields and custom fields of a given table"""

        try:
            resp = self._make_request(
                "GET", f"/services/data/{self._API_VERSION}/sobjects/{table}/describe/"
            )

            sobject = resp.json()

            fields = [
                Field(name=o["name"], type=o["type"], nullable=o["nillable"])
                for o in sobject["fields"]
            ]

            filtered = filter(
                lambda f: f.type != "json" and f.name not in self._BLACKLISTED_FIELDS,
                fields,
            )

            return {f.name: f for f in filtered}
        except requests.exceptions.HTTPError as err:
            if err.response is None:
                raise

            if not err.response.status_code == 404:
                raise

            return {}

    def get_records(
        self,
        table: str,
        fields: Dict[str, Field],
        replication_key: Optional[str],
        start_date: datetime,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> Generator[Dict, None, None]:
        field_names = list(fields.keys())

        select_stm = f"SELECT {','.join(field_names)} "
        from_stm = f"FROM {table} "

        if replication_key is not None:
            where_stm = f"WHERE {replication_key} >= {start_date.strftime('%Y-%m-%dT%H:%M:%SZ')} "
            if end_date:
                where_stm += f" AND {replication_key} < {end_date.strftime('%Y-%m-%dT%H:%M:%SZ')} "

            order_by_stm = f"ORDER BY {replication_key} ASC "
        else:
            where_stm = ""
            order_by_stm = ""

        if limit:
            limit_stm = f"LIMIT {limit}"
        else:
            limit_stm = ""

        LOGGER.info(
            f"""
            {select_stm}
            {from_stm}
            {where_stm}
            {order_by_stm}
            {limit_stm}
        """
        )

        query = f"{select_stm}{from_stm}{where_stm}{order_by_stm}{limit_stm}"

        yield from self._paginate(
            "GET",
            f"/services/data/{self._API_VERSION}/queryAll/",
            params={"q": query},
        )

    def _paginate(
        self, method: str, path: str, data: Dict = None, params: Dict = None
    ) -> Generator[Dict, None, None]:
        while path:
            resp = self._make_request(method, path, data=data, params=params)

            data = resp.json()

            for record in data.get("records", []):
                yield record

            path = data.get("nextRecordsUrl")

        return

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.HTTPError,
        ),
        max_tries=5,
        factor=2,
        on_backoff=log_backoff_attempt,
    )
    def _make_request(self, method, path, data=None, params=None) -> requests.Response:
        now = datetime.utcnow()

        if self._token_expiration_time is None or self._token_expiration_time < now:
            self._login()

        headers = {"Authorization": "Bearer {}".format(self._access_token)}

        url = f"{self._instance_url}{path}"
        resp = self.session.request(
            method, url, headers=headers, params=params, data=data
        )

        resp.raise_for_status()

        self._metrics_http_requests += 1
        self._check_rest_quota_usage(resp.headers)

        return resp

    def _login(self):
        if self.is_sandbox:
            login_url = "https://test.salesforce.com/services/oauth2/token"
        else:
            login_url = "https://login.salesforce.com/services/oauth2/token"

        data = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }

        LOGGER.info("Attempting login via OAuth2")

        try:
            resp = self.session.post(
                login_url,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=data,
            )

            resp.raise_for_status()

            LOGGER.info("OAuth2 login successful")
            auth = resp.json()

            self._access_token = auth["access_token"]
            self._instance_url = auth["instance_url"]

            self._token_expiration_time = datetime.utcnow() + timedelta(
                seconds=self._REFRESH_TOKEN_EXPIRATION_PERIOD
            )
        except requests.exceptions.HTTPError as req_ex:
            response_text = None
            if req_ex.response:
                response_text = req_ex.response.text
                LOGGER.exception(response_text or str(req_ex))
                if req_ex.response.status_code == 403:
                    raise TapSalesforceOauthException(
                        f"invalid oauth2 credentials: {req_ex.response.text}"
                    )
            raise TapSalesforceOauthException(
                "failed to refresh or login using oauth2 credentials"
            )

    def _check_rest_quota_usage(self, headers):
        match = re.search(r"^api-usage=(\d+)/(\d+)$", headers.get("Sforce-Limit-Info"))

        if match is None:
            return

        used, total = map(int, match.groups())

        LOGGER.info(
            f"Used {used / total * 100:.2f}% of daily REST API quota",
        )

        used_percent = (used / total) * 100.0

        max_requests = int((self.quota_percent_per_run * total) / 100)

        if used_percent > self.quota_percent_total:
            total_message = (
                "Salesforce has reported {}/{} ({:3.2f}%) total REST quota "
                + "used across all Salesforce Applications. Terminating "
                + "replication to not continue past configured percentage "
                + "of {}% total quota."
            ).format(used, total, used_percent, self.quota_percent_total)
            raise TapSalesforceQuotaExceededException(total_message)
        elif self._metrics_http_requests > max_requests:
            partial_message = (
                "This replication job has made {} REST requests ({:3.2f}% of "
                + "total quota). Terminating replication due to total "
                + "quota of {}% per replication."
            ).format(
                self._metrics_http_requests,
                (self._metrics_http_requests / total) * 100,
                self.quota_percent_per_run,
            )
            raise TapSalesforceQuotaExceededException(partial_message)
