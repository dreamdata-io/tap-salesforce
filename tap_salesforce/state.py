from typing import Dict, Optional
from datetime import datetime, timezone

from pydantic import BaseModel


class State(BaseModel):
    bookmarks: Dict[str, Dict] = {}

    def set_stream_state(self, stream_id: str, key: str, value: any):

        state = self.bookmarks.get(stream_id, dict())
        if type(value) == datetime:
            state[key] = value.isoformat() + "Z"
        else:
            state[key] = value
        self.bookmarks[stream_id] = state

    def get_stream_state(
        self, stream_id: str, replication_key: str
    ) -> Optional[datetime]:
        state_timestamp = self.bookmarks.get(stream_id, dict()).get(
            replication_key
        ) or self.bookmarks.get(stream_id, dict()).get(
            "SystemModstamp"
        )  # the SystemModstamp fallback enables support for legacy state

        if not state_timestamp:
            return None

        # state_timestamp[:-1] cuts of the 'Z' from the iso8601 timestamp
        return datetime.fromisoformat(state_timestamp[:-1]).astimezone(timezone.utc)
