from kairon.events.definitions.petpooja_sync import PetpoojaSync
from kairon.exceptions import AppException
from kairon.shared.constants import CatalogSyncClass


class CatalogSyncFactory:

    __provider_implementations = {
        CatalogSyncClass.petpooja_sync: PetpoojaSync,
    }

    @staticmethod
    def get_instance(catalog_sync_class: CatalogSyncClass):
        """
        Factory to retrieve catalog provider implementation for execution.

        :param catalog_sync_class: valid catalog sync class
        """
        if catalog_sync_class not in CatalogSyncFactory.__provider_implementations.keys():
            valid_syncs = [syncs.value for syncs in CatalogSyncClass]
            raise AppException(f"{catalog_sync_class} is not a valid catalog sync class. Accepted types: {valid_syncs}")
        return CatalogSyncFactory.__provider_implementations[catalog_sync_class]