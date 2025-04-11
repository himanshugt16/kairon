from typing import Text

from dotenv import set_key

from kairon import Utility
from loguru import logger

from kairon.catalog_sync.definitions.base import CatalogSyncBase
from kairon.exceptions import AppException
from kairon.meta.processor import MetaProcessor
from kairon.shared.cognition.processor import CognitionDataProcessor
from kairon.shared.constants import EventClass
from kairon.shared.data.constant import SyncType, SYNC_STATUS
from kairon.shared.data.data_objects import Integrations
from kairon.shared.catalog_sync.catalog_sync_log_processor import CatalogSyncLogProcessor


class PetpoojaSync(CatalogSyncBase):
    """
    Validates and processes data from catalog (e.g., Petpooja) before importing it
    to knowledge vault and meta
    """

    def __init__(self, bot: Text, user: Text, provider: Text, **kwargs):
        """
        Initialise event.
        """
        self.bot = bot
        self.user = user
        self.provider = provider
        self.token = kwargs.get("token", "")
        self.sync_type = kwargs.get("sync_type", SyncType.item_toggle)
        self.data = []

    def validate(self, **kwargs):
        """
        Validates if an event is already running for that particular bot and
        checks if the event trigger limit has been exceeded.
        Then, preprocesses the received request
        """

        request = kwargs.get("request_body")
        CatalogSyncLogProcessor.is_sync_in_progress(self.bot)
        # CatalogSyncLogProcessor.is_limit_exceeded(self.bot)
        CatalogSyncLogProcessor.add_log(self.bot, self.user, self.provider, self.sync_type,
                                               sync_status=SYNC_STATUS.INITIATED.value, raw_payload=request)
        if CatalogSyncLogProcessor.is_catalog_collection_exists(self.bot) is False:
            CatalogSyncLogProcessor.create_catalog_collection(bot=self.bot, user=self.user, data=self.data)
        CatalogSyncLogProcessor.add_log(self.bot, self.user, sync_status=SYNC_STATUS.VALIDATING_REQUEST)
        if self.sync_type == SyncType.push_menu:
            CatalogSyncLogProcessor.validate_item_ids(request)
            CatalogSyncLogProcessor.validate_item_fields(request, "metadata/catalog_metadata.yml")
            CatalogSyncLogProcessor.validate_image_configurations(self.bot, request)
        else:
            CatalogSyncLogProcessor.validate_item_toggle_request(request)
        return self.preprocess(request_body = request)

    def preprocess(self, **kwargs):
        """
        Transform and preprocess incoming payload data into `self.data`
        for catalog sync and meta sync.
        """
        CatalogSyncLogProcessor.add_log(self.bot, self.user, sync_status=SYNC_STATUS.PREPROCESSING)
        request = kwargs.get("request_body")
        if self.sync_type == SyncType.push_menu:
            self.data = CognitionDataProcessor.preprocess_push_menu_data(self.bot, request, "metadata/catalog_metadata.yml")
        else:
            self.data = CognitionDataProcessor.preprocess_item_toggle_data(request, "metadata/catalog_metadata.yml")
        CatalogSyncLogProcessor.add_log(self.bot, self.user, sync_status=SYNC_STATUS.PREPROCESSING_COMPLETED, processed_payload= self.data)
        CognitionDataProcessor.save_kv_data(self.data, self.bot, self.user)
        return True

    def enqueue(self, **kwargs):
        """
        Send event to event server
        """
        try:
            if not CatalogSyncLogProcessor.is_ai_enabled(self.bot):
                CatalogSyncLogProcessor.add_log(self.bot, self.user,
                                                   exception="Sync to knowledge vault is not allowed for this bot. Contact Support!!",
                                                   sync_status=SYNC_STATUS.COMPLETED.value, status= "Success")
                raise AppException("Sync to knowledge vault is not allowed in this bot. Contact Support!!")
            payload = {
                'bot': self.bot,
                'user': self.user,
                'provider': self.provider,
                'sync_type': self.sync_type,
                'token': self.token,
                'data': self.data
            }
            CatalogSyncLogProcessor.add_log(self.bot, self.user, self.provider, self.sync_type, sync_status=SYNC_STATUS.ENQUEUED.value)
            Utility.request_event_server(EventClass.catalog_integration, payload)
        except Exception as e:
            CatalogSyncLogProcessor.delete_enqueued_event_log(self.bot)
            raise e

    async def execute(self, **kwargs):
        """
        Execute the document content import event.
        """
        self.data = kwargs.get("data", {})
        cognition_processor = CognitionDataProcessor()
        try:
            knowledge_vault_data = self.data.get("kv", [])
            CatalogSyncLogProcessor.add_log(self.bot, self.user, sync_status=SYNC_STATUS.VALIDATING_KV)
            error_summary = cognition_processor.validate_data("id", "catalog",
                                                              self.sync_type.lower(), knowledge_vault_data, self.bot)
            initiate_import = True
            status = "Success"
            if error_summary:
                initiate_import = False
                status = "Failure"
            CatalogSyncLogProcessor.add_log(self.bot, self.user, validation_errors=error_summary,
                                                sync_status=SYNC_STATUS.SAVE.value)
            if initiate_import:
                result = await cognition_processor.upsert_data_new("id", f"catalog",
                                                      self.sync_type.lower(), knowledge_vault_data, self.bot, self.user)
                remaining_primary_keys = result.get("stale_ids", [])
                integrations_doc = Integrations.objects(bot = self.bot, connector_type = self.provider, sync_type = self.sync_type).first()
                if not CatalogSyncLogProcessor.is_meta_enabled(self.bot):
                    CatalogSyncLogProcessor.add_log(self.bot, self.user,
                                                    exception="Sync to Meta is not allowed for this bot. Contact Support!!",
                                                    sync_status=SYNC_STATUS.COMPLETED.value, status="Success")
                    raise AppException("Sync to Meta is not allowed for this bot. Contact Support!!")
                if integrations_doc and 'meta_config' in integrations_doc:
                    CatalogSyncLogProcessor.add_log(self.bot, self.user,sync_status=SYNC_STATUS.SAVE_META.value)
                    meta_processor = MetaProcessor(integrations_doc.meta_config.get('access_token'), integrations_doc.meta_config.get('catalog_id'))
                    meta_payload = self.data.get("meta", [])
                    if self.sync_type == SyncType.push_menu:
                        processed_data = meta_processor.preprocess_data(meta_payload, "CREATE", "metadata/catalog_metadata.yml")

                        await meta_processor.push_meta_catalog(processed_data) # Update items of push menu will be handled in CREATE itself

                        if remaining_primary_keys:
                            delete_payload = meta_processor.preprocess_delete_data(remaining_primary_keys)
                            await meta_processor.delete_meta_catalog(delete_payload)
                    else:
                        processed_data = meta_processor.preprocess_data(meta_payload,"UPDATE", "metadata/catalog_metadata.yml")
                        await meta_processor.update_meta_catalog(processed_data)
            CatalogSyncLogProcessor.add_log(self.bot, self.user, sync_status=SYNC_STATUS.COMPLETED.value, status=status)
        except Exception as e:
            logger.error(str(e))
            CatalogSyncLogProcessor.add_log(self.bot, self.user,
                                                exception=str(e),
                                                status="Failure",
                                                sync_status=SYNC_STATUS.FAILED.value)