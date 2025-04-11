from datetime import datetime

from mongoengine import StringField, BooleanField, DateTimeField, DynamicDocument, DictField
from kairon.shared.data.signals import push_notification


@push_notification.apply
class CatalogSyncLogs(DynamicDocument):
    execution_id = StringField(required=True, unique=True)
    raw_payload = DictField(required=True)
    processed_payload = DictField(default=None)
    validation_errors = DictField(default={})
    exception = StringField(default="")
    bot = StringField(required=True)
    user = StringField(required=True)
    provider = StringField(required=True)
    sync_type = StringField(required=True)
    start_timestamp = DateTimeField(default=datetime.utcnow)
    end_timestamp = DateTimeField(default=None)
    status = StringField(default=None)
    sync_status = StringField(default="COMPLETED")

    meta = {"indexes": [{"fields": ["bot", "event_id", ("bot", "event_status", "-start_timestamp")]}]}


