
from lanbilling_addresses.apps import WAddressesImportingCronTask, WAddressSyncBrokerCommands, WAddressesImportTask


def __wasp_launcher_apps__():
	return [WAddressesImportingCronTask, WAddressSyncBrokerCommands, WAddressesImportTask]
