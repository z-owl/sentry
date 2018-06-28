from __future__ import absolute_import

from sentry.eventstream.base import EventStream
from sentry.utils import snuba


class SnubaEventStream(EventStream):
    def publish(self, event, primary_hash, **kwargs):
        snuba.insert_raw([{
            'group_id': event.group_id,
            'event_id': event.event_id,
            'project_id': event.project_id,
            'message': event.message,
            'platform': event.platform,
            'datetime': event.datetime,
            'data': event.data.data,
            'primary_hash': primary_hash,
        }])
        self.consume(event=event, primary_hash=primary_hash, **kwargs)
