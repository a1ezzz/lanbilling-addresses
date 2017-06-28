# -*- coding: utf-8 -*-
# lanbilling_addresses/task.py
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
import re
from lxml.etree import iterparse

from wasp_general.task.thread import WThreadTask, WThreadedTaskChain
from wasp_general.datetime import utc_datetime
from wasp_general.verify import verify_type

from wasp_launcher.apps import WAppsGlobals
from wasp_launcher.mongodb import WMongoConnection

from lanbilling_addresses.lanbilling import WLanbillingRPC
from lanbilling_addresses.importer import WLanbillingAddressesImporter, WAddressImportCacheSingleton


class WFIASTaskStatus:

	__addrobj_loading_status__ = None
	__addrobj_exporting_status__ = None

	def __init__(self):
		self.__start_time = utc_datetime()
		self.__records_processed = 0

	def records_processed(self):
		return self.__records_processed

	@verify_type(other=int)
	def __add__(self, other):
		self.__records_processed += other
		return self

	def start_time(self):
		return self.__start_time

	@classmethod
	def reset(cls):
		WFIASTaskStatus.__addrobj_loading_status__ = None
		WFIASTaskStatus.__addrobj_exporting_status__ = None


class WFIASAddrObjBasicTask(WThreadTask):

	def __init__(self, mongo_connection, thread_name):
		WThreadTask.__init__(self, thread_name=thread_name, join_on_stop=True, ready_to_stop=True)
		self.__mongo_connection = mongo_connection

	def mongo_collection(self):
		collection = self.__mongo_connection['AS_ADDROBJ']
		collection.create_index("AOGUID")
		return collection

	def thread_stopped(self):
		pass


class WFIASAddrObjLoadingTask(WFIASAddrObjBasicTask):

	__addrobj_xml_re__ = re.compile('^AS_ADDROBJ_(\d{8})_(.{1,36})\.[X,x][M,m][L,l]$')

	def __init__(self, mongo_connection):
		WFIASAddrObjBasicTask.__init__(self, mongo_connection, 'FIAS-AddrObj-Loading')
		self.__compact = WAppsGlobals.config.getboolean('lanbilling-addresses', 'compact_records')
		self.__compact_fields = (
			'AOID', 'AOGUID', 'PARENTGUID', 'FORMALNAME', 'SHORTNAME', 'AOLEVEL', 'ACTSTATUS'
		)

	@classmethod
	def __addrobj_xml(cls):
		xml_directory = WAppsGlobals.config['lanbilling-addresses']['fias_directory']
		xml_filename = None
		re_obj = WFIASAddrObjLoadingTask.__addrobj_xml_re__

		for entry in os.listdir(xml_directory):
			filename = os.path.join(xml_directory, entry)
			if os.path.isfile(filename) is True and re_obj.match(entry) is not None:
				if xml_filename is None:
					xml_filename = entry
				else:
					raise RuntimeError(
						'Unable to find XML file (AS_ADDROBJ). Multiple files spotted'
					)

		if xml_filename is None:
			raise RuntimeError(
				"Unable to find XML file (AS_ADDROBJ). Unable to find a file in a directory %s" %
				xml_directory
			)
		return os.path.join(xml_directory, xml_filename)

	def __addrobj_iterate(self):
		for event, elem in iterparse(self.__addrobj_xml(), tag='Object'):
			yield elem

	def __load_addrobj(self, entry, mongo_collection):
		if 'ACTSTATUS' in entry.attrib and entry.attrib['ACTSTATUS'] == '1':
			if self.__compact is True:
				mongo_record = {}
				for key in self.__compact_fields:
					if key in entry.attrib:
						mongo_record[key] = entry.attrib[key]
			else:
				mongo_record = dict(entry.attrib)
			mongo_collection.insert_one(mongo_record)
		self.__cleanup_entry(entry)
		WFIASTaskStatus.__addrobj_loading_status__ += 1

	@classmethod
	def __cleanup_entry(cls, entry):
		entry.clear()
		while entry.getprevious() is not None:
			del entry.getparent()[0]
		del entry

	def thread_started(self):
		WAppsGlobals.log.info('Loading data from FIAS XML')
		if WFIASTaskStatus.__addrobj_loading_status__ is not None:
			raise RuntimeError('Multiple FIAS loading tasks spotted')

		WFIASTaskStatus.__addrobj_loading_status__ = WFIASTaskStatus()

		mongo_collection = self.mongo_collection()
		WAppsGlobals.log.info('Clearing mongo-collection')
		mongo_collection.delete_many({})

		WAppsGlobals.log.info('Loading data')
		for entry in self.__addrobj_iterate():
			if self.stop_event().is_set() is True:
				WAppsGlobals.log.warn("Fias loading task terminated. Importing wasn't complete")
				return
			self.__load_addrobj(entry, mongo_collection)

		WAppsGlobals.log.info('FIAS loaded successfully from XML')


class WFIASAddrObjExportingTask(WFIASAddrObjBasicTask):

	def __init__(self, mongo_connection):
		WFIASAddrObjBasicTask.__init__(self, mongo_connection, 'FIAS-AddrObj-Exporting')

		login = WAppsGlobals.config['lanbilling-addresses']['login'].strip()
		if len(login) == 0:
			login = None

		password = WAppsGlobals.config['lanbilling-addresses']['password'].strip()
		if len(password) == 0:
			password = None

		hostname = WAppsGlobals.config['lanbilling-addresses']['hostname'].strip()
		if len(hostname) == 0:
			hostname = None

		wsdl_url = WAppsGlobals.config['lanbilling-addresses']['wsdl_url'].strip()
		if len(wsdl_url) == 0:
			wsdl_url = None

		soap_proxy_address = WAppsGlobals.config['lanbilling-addresses']['soap_proxy_address'].strip()
		if len(soap_proxy_address) == 0:
			soap_proxy_address = None
			soap_proxy = False
		else:
			soap_proxy = True

		self.__rpc_client = WLanbillingRPC(
			hostname=hostname, login=login, password=password,
			wsdl_url=wsdl_url, soap_proxy_address=soap_proxy_address, soap_proxy=soap_proxy
		)

	@classmethod
	def __import_addrobj(cls, lanbilling_rpc, mongo_record, mongo_collection):
		WLanbillingAddressesImporter.import_address(lanbilling_rpc, mongo_record, mongo_collection)
		WFIASTaskStatus.__addrobj_exporting_status__ += 1

	def thread_started(self):
		WAppsGlobals.log.info('Exporting FIAS data to Lanbilling')

		if WFIASTaskStatus.__addrobj_exporting_status__ is not None:
			raise RuntimeError('Multiple FIAS exporting tasks spotted')
		WFIASTaskStatus.__addrobj_exporting_status__ = WFIASTaskStatus()

		WAppsGlobals.log.info('Clearing cache')
		WAddressImportCacheSingleton.storage.clear()

		mongo_collection = self.mongo_collection()

		for mongo_record in mongo_collection.find():
			if self.stop_event().is_set() is True:
				WAppsGlobals.log.warn("Fias import terminated. Importing wasn't complete")
				return
			self.__import_addrobj(self.__rpc_client, mongo_record, mongo_collection)

		WAppsGlobals.log.info('FIAS imported successfully')

		WAppsGlobals.log.info('Clearing cache')
		WAddressImportCacheSingleton.storage.clear()


class WFIASExportingTask(WThreadedTaskChain):

	def __init__(self):
		connection = self.mongo_connection()
		loading_task = WFIASAddrObjLoadingTask(connection)
		exporting_task = WFIASAddrObjExportingTask(connection)
		WThreadedTaskChain.__init__(self, loading_task, exporting_task, thread_name='FIAS-Export')
		self.__connection = connection

	@classmethod
	def mongo_connection(cls):
		return WMongoConnection.create(
			'lanbilling-addresses', 'mongo_connection', 'mongo_database'
		)

	def thread_started(self):
		WFIASTaskStatus.reset()
		WThreadedTaskChain.thread_started(self)

	def thread_stopped(self):
		WThreadedTaskChain.thread_stopped(self)
		WFIASTaskStatus.reset()
