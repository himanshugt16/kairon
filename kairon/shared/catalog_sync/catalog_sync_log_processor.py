import json
from datetime import datetime

from bson import ObjectId
from loguru import logger
from mongoengine import Q, DoesNotExist

from kairon.shared.cognition.data_objects import CognitionSchema, ColumnMetadata, CollectionData
from kairon.shared.cognition.processor import CognitionDataProcessor
from kairon.shared.content_importer.data_objects import ContentValidationLogs
from kairon.shared.data.constant import SYNC_STATUS, SyncType
from kairon.exceptions import AppException
from kairon.shared.data.data_models import CognitionSchemaRequest
from kairon.shared.data.data_objects import BotSettings, BotSyncConfig
from kairon.shared.data.processor import MongoProcessor
from kairon.shared.catalog_sync.data_objects import CatalogSyncLogs, CatalogProviderMapping
from kairon.shared.models import CognitionMetadataType


class CatalogSyncLogProcessor:
    """
    Log processor for content importer event.
    """

    @staticmethod
    def add_log(bot: str, user: str, provider: str = None, sync_type: str = None, validation_errors: dict = None,
                raw_payload: dict = None, processed_payload: dict = None, exception: str = None, status: str = None, sync_status: str = SYNC_STATUS.INITIATED.value):
        """
        Adds or updates log for content importer event.
        @param bot: bot id.
        @param user: kairon username.
        @param provider: provider (e.g. Petpooja, Shopify etc.)
        @param sync_type: sync type
        @param validation_errors: Dictionary containing any validation errors encountered
        @param exception: Exception occurred during event.
        @param status: Validation success or failure.
        @param sync_status: Event success or failed due to any error during validation or import.
        @return:
        """
        try:
            doc = CatalogSyncLogs.objects(bot=bot).filter(
                Q(sync_status__ne=SYNC_STATUS.COMPLETED.value) &
                Q(sync_status__ne=SYNC_STATUS.FAILED.value)).get()
        except DoesNotExist:
            doc = CatalogSyncLogs(
                bot=bot,
                user=user,
                execution_id=str(ObjectId()),
                provider=provider,
                raw_payload = raw_payload,
                sync_type = sync_type,
                start_timestamp=datetime.utcnow()
            )
        doc.sync_status = sync_status
        if processed_payload:
            doc.processed_payload = processed_payload
        if exception:
            doc.exception = exception
        if status:
            doc.status = status
        if validation_errors:
            doc.validation_errors = validation_errors
        if sync_status in {SYNC_STATUS.FAILED.value, SYNC_STATUS.COMPLETED.value}:
            doc.end_timestamp = datetime.utcnow()
        doc.save()

    @staticmethod
    def is_sync_in_progress(bot: str, raise_exception=True):
        """
        Checks if event is in progress.
        @param bot: bot id
        @param raise_exception: Raise exception if event is in progress.
        @return: boolean flag.
        """
        in_progress = False
        try:
            CatalogSyncLogs.objects(bot=bot).filter(
                Q(sync_status__ne=SYNC_STATUS.COMPLETED.value) &
                Q(sync_status__ne=SYNC_STATUS.FAILED.value) &
                Q(sync_status__ne=SYNC_STATUS.ABORTED.value)).get()

            if raise_exception:
                raise AppException("Sync already in progress! Check logs.")
            in_progress = True
        except DoesNotExist as e:
            logger.error(e)
        return in_progress

    @staticmethod
    def is_limit_exceeded(bot: str, raise_exception=True):
        """
        Checks if daily event triggering limit exceeded.
        @param bot: bot id.
        @param raise_exception: Raise exception if limit is reached.
        @return: boolean flag
        """
        today = datetime.today()

        today_start = today.replace(hour=0, minute=0, second=0)
        doc_count = CatalogSyncLogs.objects(
            bot=bot, start_timestamp__gte=today_start
        ).count()
        if doc_count >= BotSettings.objects(bot=bot).get().content_importer_limit_per_day:
            if raise_exception:
                raise AppException("Daily limit exceeded.")
            else:
                return True
        else:
            return False

    @staticmethod
    def get_logs(bot: str, start_idx: int = 0, page_size: int = 10):
        """
        Get all logs for content importer event.
        @param bot: bot id.
        @param start_idx: start index
        @param page_size: page size
        @return: list of logs.
        """
        for log in CatalogSyncLogs.objects(bot=bot).order_by("-start_timestamp").skip(start_idx).limit(page_size):
            log = log.to_mongo().to_dict()
            log.pop('_id')
            log.pop('bot')
            log.pop('user')
            yield log

    @staticmethod
    def delete_enqueued_event_log(bot: str):
        """
        Deletes latest log if it is present in enqueued state.
        """
        latest_log = CatalogSyncLogs.objects(bot=bot).order_by('-id').first()
        if latest_log and latest_log.sync_status == SYNC_STATUS.ENQUEUED.value:
            latest_log.delete()

    @staticmethod
    def is_catalog_collection_exists(bot: str) -> bool:
        """
        Checks if the 'catalogue_table' exists in CognitionSchema for the given bot.
        """
        return CognitionSchema.objects(bot=bot, collection_name="catalog").first() is not None

    @staticmethod
    def create_catalog_collection(bot: str, user: str, data):
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
            collection_name="catalog",
            metadata=metadata
        )

        metadata_id = cognition_processor.save_cognition_schema(
            catalog_schema.dict(),
            user, bot)

        return metadata_id

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
    def validate_item_toggle_request(json_data: dict) -> None:
        """
        Validates that `inStock` exists and is a boolean,
        and `itemID` exists in the payload.

        Raises:
            ValueError: If any validation fails.
        """
        body = json_data.get("body", {})

        if "inStock" not in body:
            raise AppException("Missing required field: 'inStock'")
        if not isinstance(body["inStock"], bool):
            raise AppException("'inStock' must be a boolean (true or false)")

        if "itemID" not in body:
            raise AppException("Missing required field: 'itemID'")

    @staticmethod
    def validate_item_fields(bot, json_data, provider):
        """
        Validates that each item has the required source fields as defined in the metadata file.
        Ensures 'item_categoryid' is within valid categories.
        Only runs if event_type is 'push_menu'.
        """
        doc = CatalogProviderMapping.objects(bot=bot, provider=provider).first()
        if not doc:
            raise ValueError(f"Metadata mappings not found for bot={bot} and provider={provider}")

        provider_mappings = {
            "meta": doc.meta_mappings,
            "kv": doc.kv_mappings
        }

        valid_category_ids = {cat["categoryid"] for cat in json_data.get("categories", [])}

        required_fields = set()
        for system_fields in provider_mappings.values():
            for config in system_fields.values():
                source_field = config.get("source")
                if source_field:
                    required_fields.add(source_field)

        for item in json_data.get("items", []):
            missing_fields = [field for field in required_fields if field not in item]
            if missing_fields:
                raise AppException(f"Missing fields {missing_fields} in item: {item}")

            if "item_categoryid" in item and item["item_categoryid"] not in valid_category_ids:
                raise AppException(f"Invalid 'item_categoryid' {item['item_categoryid']} in item: {item}")

    @staticmethod
    def is_sync_type_allowed(bot: str, sync_type: str):
        config = BotSyncConfig.objects(branch_bot=bot).first()
        if not config:
            raise AppException("No bot sync config found for bot")

        if sync_type == SyncType.push_menu and not config.process_push_menu:
            raise AppException("Push menu processing is disabled for this bot.")

        if sync_type == SyncType.item_toggle and not config.process_item_toggle:
            raise AppException("Item toggle is disabled for this bot.")


    @staticmethod
    def is_ai_enabled(bot: str):
        config = BotSyncConfig.objects(branch_bot=bot).first()
        if not config:
            raise AppException("No bot sync config found for bot")
        return config.ai_enabled

    @staticmethod
    def is_meta_enabled(bot: str):
        config = BotSyncConfig.objects(branch_bot=bot).first()
        if not config:
            raise AppException("No bot sync config found for bot")
        return config.meta_enabled

    @staticmethod
    def validate_image_configurations(bot: str, user: str):
        """
        Validates image configuration before preprocessing:

        - If `is_enabled` is True:
            - `image_s3_url` must be present in the bot's sync config.

        - If `is_enabled` is False:
            - Catalog collection `<Customer>_<Branch>_catalog_images` must exist.
            - Each `item_id` must have a document with a valid `image_s3_url`.
        """

        config = BotSyncConfig.objects(branch_bot=bot).first()
        if not config:
            raise AppException(f"No bot sync config found for bot: {bot}")

        customer = config.customer.replace(" ", "_")
        branch = config.branch_name.replace(" ", "_")
        catalog_images_collection = f"{customer}_{branch}_catalog_images"

        if not CollectionData.objects(collection_name=catalog_images_collection).first():
            raise AppException(f"Collection `{catalog_images_collection}` does not exist for bot: {bot}")

        if not CollectionData.objects(collection_name=catalog_images_collection).first():
            fallback_data = {
                "itemid": f"{bot}global",
                "image_url": ""  # You may want to populate this if needed
            }
            CollectionData(
                collection_name=catalog_images_collection,
                data=fallback_data,
                user=user,
                bot=bot,
                status=True,
                timestamp=datetime.utcnow()
            ).save()

        fallback_item_id = f"{bot}_global"
        document = CollectionData.objects(
            collection_name=catalog_images_collection,
            data__itemid=fallback_item_id
        ).first()

        if not document:
            raise AppException(
                f"Fallback image document `{fallback_item_id}` not found in `{catalog_images_collection}`")
