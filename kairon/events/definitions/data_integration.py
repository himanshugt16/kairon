from typing import Text
from kairon import Utility
from loguru import logger
from kairon.events.definitions.base import EventsBase
from kairon.meta.processor import MetaProcessor
from kairon.shared.cognition.processor import CognitionDataProcessor
from kairon.shared.constants import EventClass
from kairon.shared.data.constant import EVENT_STATUS
from kairon.shared.data.data_objects import Integrations
from kairon.shared.integrations.integration_log_processor import DataIntegrationLogProcessor


class DataIntegrationEvent(EventsBase):
    """
    Validates and processes data from integrations (e.g., Petpooja) before importing it
    to knowledge vault and meta
    """

    def __init__(self, bot: Text, user: Text, integration: Text, **kwargs):
        """
        Initialise event.
        """
        self.bot = bot
        self.user = user
        self.integration = integration
        self.token = kwargs.get("token", "")
        self.event_type = kwargs.get("event_type", "field_update")
        self.data = []

    def validate(self, **kwargs):
        """
        Validates if an event is already running for that particular bot and
        checks if the event trigger limit has been exceeded.
        Then, preprocesses the received request
        """
        is_event_data = True
        request = kwargs.get("request_body")
        DataIntegrationLogProcessor.is_event_in_progress(self.bot)
        # DataIntegrationLogProcessor.is_limit_exceeded(self.bot)
        if DataIntegrationLogProcessor.is_catalog_collection_exists(self.bot) is False:
            DataIntegrationLogProcessor.create_catalog_collection(bot=self.bot, user=self.user)
        DataIntegrationLogProcessor.validate_item_ids(request)
        DataIntegrationLogProcessor.validate_item_fields(request, self.event_type)
        self.data = CognitionDataProcessor.preprocess_menu_data(request, self.event_type)
        return is_event_data

    def enqueue(self, **kwargs):
        """
        Send event to event server
        """
        payload = {
            'bot': self.bot,
            'user': self.user,
            'integration': self.integration,
            'event_type': self.event_type,
            'token': self.token,
            'data': self.data
        }
        DataIntegrationLogProcessor.add_log(self.bot, self.user, self.integration, self.event_type, event_status=EVENT_STATUS.ENQUEUED.value)
        try:
            Utility.request_event_server(EventClass.data_integration, payload)
        except Exception as e:
            DataIntegrationLogProcessor.delete_enqueued_event_log(self.bot)
            raise e

    async def execute(self, **kwargs):
        """
        Execute the document content import event.
        """
        self.data = kwargs.get("data", [])
        cognition_processor = CognitionDataProcessor()
        try:
            self.data = [{key.lower(): value for key, value in row.items()} for row in self.data]
            knowledge_vault_data = DataIntegrationLogProcessor.extract_knowledge_vault_data(self.data, self.event_type)
            DataIntegrationLogProcessor.add_log(self.bot, self.user, event_status=EVENT_STATUS.VALIDATING.value)
            error_summary = cognition_processor.validate_data("id", "catalog",
                                                              self.event_type.lower(), knowledge_vault_data, self.bot)
            initiate_import = True
            status = "Success"
            if error_summary:
                initiate_import = False
                status = "Failure"
            DataIntegrationLogProcessor.add_log(self.bot, self.user, validation_errors=error_summary,
                                                event_status=EVENT_STATUS.SAVE.value)
            if initiate_import:
                await cognition_processor.upsert_data("id", "catalog",
                                                      self.event_type.lower(), knowledge_vault_data, self.bot, self.user)
                integrations_doc = Integrations.objects(bot = self.bot, connector_type = self.integration, event_type = self.event_type).first()
                if integrations_doc and 'meta_config' in integrations_doc:
                    meta_processor = MetaProcessor(integrations_doc.meta_config.get('access_token'), integrations_doc.meta_config.get('catalog_id'))

                    if self.event_type == "push_menu":
                        processed_data = meta_processor.preprocess_data(self.data, "CREATE")
                        await meta_processor.push_meta_catalog(processed_data)
                    else:
                        processed_data = meta_processor.preprocess_data(self.data,"UPDATE")
                        await meta_processor.update_meta_catalog(processed_data)
            DataIntegrationLogProcessor.add_log(self.bot, self.user, event_status=EVENT_STATUS.COMPLETED.value, status=status)
        except Exception as e:
            logger.error(str(e))
            DataIntegrationLogProcessor.add_log(self.bot, self.user,
                                                exception=str(e),
                                                status="Failure",
                                                event_status=EVENT_STATUS.FAIL.value)