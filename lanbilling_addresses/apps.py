# -*- coding: utf-8 -*-
# lanbilling_addresses/apps.py
#
# Copyright (C) 2016 the lanbilling-addresses authors and contributors
# <see AUTHORS file>
#
# This file is part of lanbilling-addresses.
#
# lanbilling-addresses is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# lanbilling-addresses is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with lanbilling-addresses.  If not, see <http://www.gnu.org/licenses/>.

# TODO: document the code
# TODO: write tests for the code

# noinspection PyUnresolvedReferences
from lanbilling_addresses.version import __author__, __version__, __credits__, __license__, __copyright__, __email__
# noinspection PyUnresolvedReferences
from lanbilling_addresses.version import __status__

import os
import signal

from wasp_general.task.scheduler.proto import WScheduledTask
from wasp_general.command.command import WCommand, WCommandResult

from wasp_launcher.apps import WCronTasks, WBrokerCommands, WGuestApp

from lanbilling_addresses.task import WFIASImportingTask


class WFIASScheduledTask(WFIASImportingTask, WScheduledTask):

	def __init__(self):
		WFIASImportingTask.__init__(self)
		WScheduledTask.__init__(self, thread_name_suffix='FIAS-import')


class WAddressesImportingCronTask(WCronTasks):

	__registry_tag__ = 'com.binblob.lanbilling-addresses.cron-app.fias-import'
	__cron_tasks__ = [WFIASScheduledTask()]


class WAddressesImportTask(WGuestApp, WFIASImportingTask):

	__registry_tag__ = 'com.binblob.lanbilling-addresses.fias-import'

	def __init__(self):
		WGuestApp.__init__(self)
		WFIASImportingTask.__init__(self)

	def start(self):
		WFIASImportingTask.start(self)
		os.kill(os.getpid(), signal.SIGINT)


class WAddressesImportCommands:

	class ImportStatus(WCommand):

		def __init__(self):
			WCommand.__init__(self, 'apps', 'fias-import', 'status')

		def _exec(self, *command_tokens):
			if WFIASImportingTask.__import_meta__ is None:
				return WCommandResult(output="Import doesn't run at the moment")

			mongo_count = WFIASImportingTask.addrobj_mongo_collection().count()

			output = 'Import started at: %s UTC\n' % WFIASImportingTask.__import_meta__.start_time()
			output += 'Records loaded: %i\n' % WFIASImportingTask.__import_meta__.records_loaded()
			output += 'Records at the mongo database: %i\n' % mongo_count
			output += 'Records imported: %i' % WFIASImportingTask.__import_meta__.records_imported()

			return WCommandResult(output=output)


class WAddressSyncBrokerCommands(WBrokerCommands):

	__registry_tag__ = 'com.binblob.lanbilling-addresses.broker-commands'

	@classmethod
	def commands(cls):
		return [WAddressesImportCommands.ImportStatus()]
