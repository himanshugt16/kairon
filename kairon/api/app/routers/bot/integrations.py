from typing import  Text

from fastapi import Security, APIRouter, Path
from starlette.requests import Request

from kairon.api.models import Response
from kairon.events.definitions.petpooja_sync import PetpoojaSync
from kairon.shared.auth import Authentication
from kairon.shared.cognition.processor import CognitionDataProcessor
from kairon.shared.constants import CatalogProvider
from kairon.shared.constants import DESIGNER_ACCESS
from kairon.shared.data.data_objects import BotSyncConfig
from kairon.shared.catalog_sync.catalog_sync_log_processor import CatalogSyncLogProcessor
from kairon.shared.models import User

router = APIRouter()
cognition_processor = CognitionDataProcessor()

@router.post("/{provider}/{sync_type}/{bot}/{token}", response_model=Response)
async def sync_data(
    request: Request,
    provider: CatalogProvider = Path(description="Catalog provider name",
                                 examples=[CatalogProvider.PETPOOJA.value]),
    bot: Text = Path(description="Bot id"),
    sync_type: Text = Path(description="Sync Type"),
    current_user: User = Security(Authentication.get_current_user_and_bot, scopes=DESIGNER_ACCESS),
    token: str = Path(description="JWT token for authentication"),
):
    """
    Handles incoming data from catalog_sync (e.g., Petpooja) for processing, validation, and eventual storage.
    """
    # bot_sync_config = BotSyncConfig(
    #     process_push_menu=True,
    #     process_item_toggle=False,
    #     parent_bot="67d7dae80630010fcb6b24ea",
    #     customer="rest",
    #     point_of_sale="petpooja",
    #     branch_name="branch",
    #     branch_bot="67d7dae80630010fcb6b24ea",
    #     ai_enabled=False,
    #     meta_enabled=False,
    #     default_logo_s3={"is_enabled": True,
    #                      "image_s3_url": "https://marketplace.canva.com/EAFaFUz4aKo/3/0/1600w/canva-yellow-abstract-cooking-fire-free-logo-tn1zF-_cG9c.jpg"},
    #     user="himanshu.gupta@nimblework.com"
    # )
    #
    # # Save it to the MongoDB collection
    # bot_sync_config.save()

    CatalogSyncLogProcessor.is_sync_type_allowed(bot, sync_type)

    request_body = await request.json()

    event = PetpoojaSync(
        bot=bot,
        user=current_user.get_user(),
        provider = provider,
        sync_type = sync_type,
        token = token
    )
    is_event_data = event.validate(request_body=request_body)
    if is_event_data:
        event.enqueue()
    return {"message": "Sync in progress! Check logs."}