from typing import  Text

from fastapi import Security, APIRouter, Path
from starlette.requests import Request

from kairon.api.models import Response
from kairon.events.definitions.data_integration import CatalogIntegrationEvent
from kairon.shared.auth import Authentication
from kairon.shared.cognition.processor import CognitionDataProcessor
from kairon.shared.constants import DataIntegrationTypes
from kairon.shared.constants import DESIGNER_ACCESS
from kairon.shared.integrations.integration_log_processor import CatalogIntegrationLogProcessor
from kairon.shared.models import User

router = APIRouter()
cognition_processor = CognitionDataProcessor()

@router.post("/{integration}/{event_type}/{bot}/{token}", response_model=Response)
async def sync_data(
    request: Request,
    integration: DataIntegrationTypes = Path(description="Data Integration name",
                                 examples=[DataIntegrationTypes.PETPOOJA.value]),
    bot: Text = Path(description="Bot id"),
    event_type: Text = Path(description="Event Type"),
    current_user: User = Security(Authentication.get_current_user_and_bot, scopes=DESIGNER_ACCESS),
    token: str = Path(description="JWT token for authentication"),
):
    """
    Handles incoming data from integrations (e.g., Petpooja) for processing, validation, and eventual storage.
    """
    CatalogIntegrationLogProcessor.is_event_type_allowed(bot, event_type)

    request_body = await request.json()

    event = CatalogIntegrationEvent(
        bot=bot,
        user=current_user.get_user(),
        integration = integration,
        event_type = event_type,
        token = token
    )
    is_event_data = event.validate(request_body=request_body)
    if is_event_data:
        event.enqueue()
    return {"message": "Sync in progress! Check logs."}