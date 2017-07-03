
from lanbilling_addresses.apps import WAddressesExportingCronTask, WAddressSyncBrokerCommands, WAddressesExportTask


def __wasp_launcher_apps__():
	return [WAddressesExportingCronTask, WAddressSyncBrokerCommands, WAddressesExportTask]
