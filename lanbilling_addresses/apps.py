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

from wasp_general.datetime import utc_datetime
from wasp_general.task.scheduler.proto import WScheduledTask
from wasp_general.command.command import WCommand, WCommandResult

from wasp_launcher.apps import WCronTasks, WBrokerCommands, WGuestApp, WThreadTaskLoggingHandler

from lanbilling_addresses.task import WFIASExportingTask, WFIASTaskStatus
from lanbilling_addresses.exporter import WAddressExportCacheSingleton, WGUIDCacheRecord


class WFIASScheduledTask(WFIASExportingTask, WScheduledTask, WThreadTaskLoggingHandler):

	def __init__(self):
		WFIASExportingTask.__init__(self)
		WScheduledTask.__init__(self, thread_name_suffix='FIAS-export')
		WThreadTaskLoggingHandler.__init__(self)

	def thread_exception(self, raised_exception):
		WThreadTaskLoggingHandler.thread_exception(self, raised_exception)


class WAddressesExportingCronTask(WCronTasks):

	__registry_tag__ = 'com.binblob.lanbilling-addresses.cron-app.fias-export'

	@classmethod
	def _cron_tasks(cls):
		return [WFIASScheduledTask()]


class WAddressesExportTask(WGuestApp, WFIASExportingTask):

	__registry_tag__ = 'com.binblob.lanbilling-addresses.fias-export'

	def __init__(self):
		WGuestApp.__init__(self)
		WFIASExportingTask.__init__(self)

	def thread_started(self):
		WFIASExportingTask.thread_started(self)
		if self.stop_event().is_set() is False:
			os.kill(os.getpid(), signal.SIGINT)

	def stop(self):
		WFIASExportingTask.stop(self)

	def thread_exception(self, raised_exception):
		WFIASExportingTask.thread_exception(self, raised_exception)
		os.kill(os.getpid(), signal.SIGINT)


class WAddressesExportCommands:

	class ExportStatus(WCommand):

		def __init__(self):
			WCommand.__init__(self, 'apps', 'fias-export', 'status')

		def _exec(self, *command_tokens):
			if WFIASTaskStatus.__addrobj_loading_status__ is None:
				return WCommandResult(output="Export doesn't run at the moment")

			connection = WFIASExportingTask.mongo_connection()
			mongo_count = connection['AS_ADDROBJ'].count()

			load_start = WFIASTaskStatus.__addrobj_loading_status__.start_time()
			records_loaded = WFIASTaskStatus.__addrobj_loading_status__.records_processed()
			output = 'Load started at: %s UTC\n' % load_start
			output += 'Records loaded: %i\n' % records_loaded

			export_start = None
			if WFIASTaskStatus.__addrobj_exporting_status__ is not None:
				export_start = WFIASTaskStatus.__addrobj_exporting_status__.start_time()
			load_duration = (export_start if export_start is not None else utc_datetime()) - load_start
			load_rate = records_loaded / load_duration.total_seconds()
			output += 'Loading rate: {:.4f} records per second\n'.format(load_rate)

			output += 'Records at the mongo database: %i\n' % mongo_count

			if export_start is not None:
				output += 'Export started at: %s UTC\n' % export_start

				records_exported = WFIASTaskStatus.__addrobj_exporting_status__.records_processed()
				output += 'Records exported: %i\n' % records_exported

				export_duration = utc_datetime() - export_start
				export_rate = records_exported / export_duration.total_seconds()
				output += 'Export rate: {:.4f} records per second\n'.format(export_rate)

				cache_hit = WAddressExportCacheSingleton.guid_cache.cache_hit()
				cache_missed = WAddressExportCacheSingleton.guid_cache.cache_missed()
				total_tries = cache_hit + cache_missed
				hit_rate = cache_hit / total_tries if total_tries > 0 else total_tries
				output += 'GUID Cache hit rate: {:.4f} (total tries: {:d}). Cache size: {:d} records\n'.format(hit_rate, total_tries, WGUIDCacheRecord.cache_size())

				cache_hit = WAddressExportCacheSingleton.rpc_cache.cache_hit()
				cache_missed = WAddressExportCacheSingleton.rpc_cache.cache_missed()
				total_tries = cache_hit + cache_missed
				hit_rate = cache_hit / total_tries if total_tries > 0 else total_tries
				output += 'RPC GET Cache hit rate: {:.4f} (total tries: {:d}). Cache size: {:d} records'.format(hit_rate, total_tries, WAddressExportCacheSingleton.rpc_cache.cache_size())

			else:
				output += "Export doesn't started"

			return WCommandResult(output=output)


class WAddressSyncBrokerCommands(WBrokerCommands):

	__registry_tag__ = 'com.binblob.lanbilling-addresses.broker-commands'

	@classmethod
	def commands(cls):
		return [WAddressesExportCommands.ExportStatus()]
