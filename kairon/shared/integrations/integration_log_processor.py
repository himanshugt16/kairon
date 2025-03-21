from datetime import datetime

from bson import ObjectId
from loguru import logger
from mongoengine import Q, DoesNotExist

from kairon.shared.cognition.data_objects import CognitionSchema, ColumnMetadata
from kairon.shared.cognition.processor import CognitionDataProcessor
from kairon.shared.content_importer.data_objects import ContentValidationLogs
from kairon.shared.data.constant import EVENT_STATUS
from kairon.exceptions import AppException
from kairon.shared.data.data_models import CognitionSchemaRequest
from kairon.shared.data.data_objects import BotSettings
from kairon.shared.data.processor import MongoProcessor
from kairon.shared.integrations.data_objects import DataIntegrationLogs
from kairon.shared.models import CognitionMetadataType


class DataIntegrationLogProcessor:
    """
    Log processor for content importer event.
    """

    @staticmethod
    def add_log(bot: str, user: str, integration: str = None, sync_type: str = None, validation_errors: dict = None,
                exception: str = None, status: str = None, event_status: str = EVENT_STATUS.INITIATED.value):
        """
        Adds or updates log for content importer event.
        @param bot: bot id.
        @param user: kairon username.
        @param integration: integration (e.g. Petpooja, Shopify etc.)
        @param sync_type: sync type
        @param validation_errors: Dictionary containing any validation errors encountered
        @param exception: Exception occurred during event.
        @param status: Validation success or failure.
        @param event_status: Event success or failed due to any error during validation or import.
        @return:
        """
        try:
            doc = DataIntegrationLogs.objects(bot=bot).filter(
                Q(event_status__ne=EVENT_STATUS.COMPLETED.value) &
                Q(event_status__ne=EVENT_STATUS.FAIL.value)).get()
        except DoesNotExist:
            doc = DataIntegrationLogs(
                bot=bot,
                user=user,
                integration=integration,
                start_timestamp=datetime.utcnow(),
                event_id=str(ObjectId())
            )
        doc.event_status = event_status
        if sync_type:
            doc.sync_type = sync_type
        if exception:
            doc.exception = exception
        if status:
            doc.status = status
        if validation_errors:
            doc.validation_errors = validation_errors
        if event_status in {EVENT_STATUS.FAIL.value, EVENT_STATUS.COMPLETED.value}:
            doc.end_timestamp = datetime.utcnow()
        doc.save()

    @staticmethod
    def is_event_in_progress(bot: str, raise_exception=True):
        """
        Checks if event is in progress.
        @param bot: bot id
        @param raise_exception: Raise exception if event is in progress.
        @return: boolean flag.
        """
        in_progress = False
        try:
            DataIntegrationLogs.objects(bot=bot).filter(
                Q(event_status__ne=EVENT_STATUS.COMPLETED.value) &
                Q(event_status__ne=EVENT_STATUS.FAIL.value) &
                Q(event_status__ne=EVENT_STATUS.ABORTED.value)).get()

            if raise_exception:
                raise AppException("Event already in progress! Check logs.")
            in_progress = True
        except DoesNotExist as e:
            logger.error(e)
        return in_progress

    # @staticmethod
    # def is_limit_exceeded(bot: str, raise_exception=True):
    #     """
    #     Checks if daily event triggering limit exceeded.
    #     @param bot: bot id.
    #     @param raise_exception: Raise exception if limit is reached.
    #     @return: boolean flag
    #     """
    #     today = datetime.today()
    #
    #     today_start = today.replace(hour=0, minute=0, second=0)
    #     doc_count = DataIntegrationLogs.objects(
    #         bot=bot, start_timestamp__gte=today_start
    #     ).count()
    #     if doc_count >= BotSettings.objects(bot=bot).get().content_importer_limit_per_day:
    #         if raise_exception:
    #             raise AppException("Daily limit exceeded.")
    #         else:
    #             return True
    #     else:
    #         return False

    @staticmethod
    def get_logs(bot: str, start_idx: int = 0, page_size: int = 10):
        """
        Get all logs for content importer event.
        @param bot: bot id.
        @param start_idx: start index
        @param page_size: page size
        @return: list of logs.
        """
        for log in DataIntegrationLogs.objects(bot=bot).order_by("-start_timestamp").skip(start_idx).limit(page_size):
            log = log.to_mongo().to_dict()
            log.pop('_id')
            log.pop('bot')
            log.pop('user')
            yield log

    # @staticmethod
    # def get_file_received_for_latest_event(bot: str):
    #     """
    #     Fetch set of files received for latest event.
    #     @param bot: bot id.
    #     """
    #     file_received = next(DataIntegrationLogProcessor.get_logs(bot)).get("file_received")
    #     return file_received

    # @staticmethod
    # def get_event_id_for_latest_event(bot: str):
    #     """
    #     Fetch event_id for latest event.
    #     @param bot: bot id.
    #     """
    #     event_id = next(DataIntegrationLogProcessor.get_logs(bot)).get("event_id")
    #     return event_id

    @staticmethod
    def delete_enqueued_event_log(bot: str):
        """
        Deletes latest log if it is present in enqueued state.
        """
        latest_log = DataIntegrationLogs.objects(bot=bot).order_by('-id').first()
        if latest_log and latest_log.event_status == EVENT_STATUS.ENQUEUED.value:
            latest_log.delete()

    @staticmethod
    def is_catalog_collection_exists(bot: str) -> bool:
        """
        Checks if the 'catalogue_table' exists in CognitionSchema for the given bot.
        """
        return CognitionSchema.objects(bot=bot, collection_name="catalog").first() is not None

    @staticmethod
    def create_catalog_collection(bot: str, user: str):
        """
        Creates a 'catalogue_table' collection in CognitionSchema for the given bot with predefined metadata fields.
        """
        # Define column names and their data types
        cognition_processor = CognitionDataProcessor()
        column_definitions = [
            ("id", CognitionMetadataType.str.value),
            ("title", CognitionMetadataType.str.value),
            ("description", CognitionMetadataType.str.value),
            ("price", CognitionMetadataType.float.value),
            ("facebook_product_category", CognitionMetadataType.str.value),
            ("availability", CognitionMetadataType.str.value),
        ]

        bot_settings = BotSettings.objects(bot=bot).first()
        if bot_settings:
            bot_settings.cognition_columns_per_collection_limit = 10
            bot_settings.llm_settings['enable_faq'] = True
            bot_settings.save()

        metadata= [
            {
                "column_name": col,
                "data_type": data_type,
                "enable_search": True,
                "create_embeddings": True
            }
            for col, data_type in column_definitions
        ]

        catalog_schema = CognitionSchemaRequest(
            collection_name = "catalog",
            metadata = metadata
        )

        metadata_id = cognition_processor.save_cognition_schema(
            catalog_schema.dict(),
            user, bot)

        return metadata_id

    # @staticmethod
    # def extract_knowledge_vault_data(data, event_type):
    #     """
    #     Extracts only the required fields for knowledge vault storage from processed menu data.
    #     """
    #     return [
    #         {
    #             "id": item["id"],
    #             "title": item["title"],
    #             "description": item["description"],
    #             "price": item["price"],
    #             "facebook_product_category": item["facebook_product_category"],
    #             "availability": item["availability"]
    #         }
    #         for item in data
    #     ]

    # @staticmethod
    # def extract_knowledge_vault_data(data, event_type):
    #     """
    #     Extracts only the required fields for knowledge vault storage from processed menu data.
    #     If event_type is "push_menu", all fields are mandatory.
    #     Otherwise, only "id" is mandatory, and other fields are included only if present.
    #     """
    #     required_fields = ["title", "description", "price", "facebook_product_category", "availability"]
    #
    #     return [
    #         {
    #             "id": item["id"],
    #             **(
    #                 {field: item[field] for field in required_fields}  # Include all fields for "push_menu"
    #                 if event_type == "push_menu"
    #                 else {field: item[field] for field in required_fields if field in item}
    #             # Only present fields otherwise
    #             )
    #         }
    #         for item in data
    #     ]

    @staticmethod
    def extract_knowledge_vault_data(data, event_type):
        """
        Extracts required fields for knowledge vault storage from processed menu data.
        - If event_type is "push_menu", include all required fields.
        - Otherwise, include only available fields along with "id".
        """
        required_fields = ["title", "description", "price", "facebook_product_category", "availability"]

        extracted_data = []
        for item in data:
            entry = {"id": item["id"]}
            if event_type == "push_menu":
                entry.update({field: item[field] for field in required_fields})
            else:
                entry.update({field: item[field] for field in required_fields if field in item})
            extracted_data.append(entry)

        return extracted_data

    @staticmethod
    def validate_item_ids(json_data):
        """
        Validates that all items have an 'itemid' and extracts a list of valid category IDs.
        Raises an exception if any item is missing 'itemid'.
        Returns a set of valid category IDs.
        """
        for item in json_data.get("items", []):
            if "itemid" not in item:
                raise AppException(f"Missing 'itemid' in item: {item}")


    @staticmethod
    def validate_item_fields(json_data, event_type):
        """
        Validates that each item has the required fields.
        Ensures 'item_categoryid' is within valid categories.
        Only runs if event_type is 'field_update'.
        """
        if event_type != "push_menu":
            return

        valid_category_ids = {cat["categoryid"] for cat in json_data.get("categories", [])}
        required_fields = ["itemname", "itemdescription", "price", "item_categoryid", "in_stock", "item_image_url"]

        for item in json_data.get("items", []):
            missing_fields = [field for field in required_fields if field not in item]
            if missing_fields:
                raise AppException(f"Missing fields {missing_fields} in item: {item}")

            if item["item_categoryid"] not in valid_category_ids:
                raise AppException(f"Invalid 'item_categoryid' {item['item_categoryid']} in item: {item}")
