from typing import  Text

from fastapi import Security, APIRouter, Path, BackgroundTasks
from starlette.requests import Request

from kairon.api.models import Response
from kairon.events.definitions.catalog_sync import CatalogSync
from kairon.events.definitions.petpooja_sync import PetpoojaSync
from kairon.exceptions import AppException
from kairon.shared.auth import Authentication
from kairon.shared.catalog_sync.data_objects import CatalogSyncLogs
from kairon.shared.cognition.processor import CognitionDataProcessor
from kairon.shared.constants import CatalogProvider
from kairon.shared.constants import DESIGNER_ACCESS
from kairon.shared.data.data_objects import BotSyncConfig
from kairon.shared.catalog_sync.catalog_sync_log_processor import CatalogSyncLogProcessor
from kairon.shared.models import User
from kairon.shared.utils import MailUtility

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



    # await MailUtility.format_and_send_mail(
    #         mail_type="catalog_sync_status", email="himanshu.gupta@nimblework.com", first_name="HG", current_status = "HG2"
    #     )



    request_body = await request.json()

    event = CatalogSync(
        bot=bot,
        user=current_user.get_user(),
        provider=provider,
        sync_type=sync_type,
        token=token
    )

    is_event_data = event.validate(request_body=request_body)
    if is_event_data:
        event.enqueue()
    return {"message": "Sync in progress! Check logs."}
