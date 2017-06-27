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

from wasp_general.task.thread import WThreadTask
from wasp_general.datetime import utc_datetime
from wasp_general.cache import cache_control, WInstanceSingletonCacheStorage

from wasp_launcher.apps import WAppsGlobals
from wasp_launcher.mongodb import WMongoConnection

from lanbilling_addresses.lanbilling import WLanbillingRPC
from lanbilling_addresses.importer import WLanbillingAddressesImporter, WGUIDCacheRecord, WAddressImportCacheSingleton


class WFIASImportingTask(WThreadTask):
	"""
	XML-features:
	- there are no single "DIVTYPE" attribute in a whole file
	- "AOLEVEL" attribute description in documentation doesn't match the real case file. In the real file there
	are the following levels: 1, 3, 4, 5, 6, 65, 7, 90, 91

1 – уровень региона
3 – уровень района
35 – уровень городских и сельских поселений
4 – уровень города
5 – уровень внутригородской территории (устаревшее)
6 – уровень населенного пункта
65 – планировочная структура
7 – уровень улицы
90 – уровень дополнительных территорий (устаревшее)
91 – уровень объектов на дополнительных территориях (устаревшее)


	XML-record example:

	AOID="ff04d0d0-0db0-4846-a3fe-ffd8f8d8c1ce"
	AOGUID="045572f0-1cd4-42d9-956d-1d27dd97662f"
	PARENTGUID="7220a42c-e12f-492d-8a1e-9e2af7b65b5f"
	FORMALNAME="Железнодорожная"
	OFFNAME="Железнодорожная"
	SHORTNAME="ул"
	AOLEVEL="7"
	REGIONCODE="99"
	AREACODE="000"
	AUTOCODE="0"
	CITYCODE="000"
	CTARCODE="000"
	PLACECODE="002"
	PLANCODE="0000"
	STREETCODE="0027"
	EXTRCODE="0000"
	SEXTCODE="000"
	PLAINCODE="990000000020027"
	CODE="99000000002002700"
	CURRSTATUS="0"
	ACTSTATUS="1"
	LIVESTATUS="1"
	CENTSTATUS="0"
	OPERSTATUS="1"
	IFNSFL="9901"
	IFNSUL="9901"
	OKATO="55000000000"
	POSTALCODE="468320"
	STARTDATE="1900-01-01"
	ENDDATE="2079-06-06"
	UPDATEDATE="2011-09-13"
	"""
	# TODO: do not import all attributes from XML (AOID, AOGUID, PARENTGUID, FORMALNAME, SHORTNAME, AOLEVEL - must be enough)

	class ImportMeta:
		def __init__(self):
			self.__connection = WMongoConnection.create(
				'lanbilling-addresses', 'mongo_connection', 'mongo_database'
			)
			self.__records_loaded = 0
			self.__records_checked = 0
			self.__load_start_time = utc_datetime()
			self.__import_start_time = None

		def connection(self):
			return self.__connection

		def records_loaded(self):
			return self.__records_loaded

		def increment_loaded(self):
			self.__records_loaded += 1

		def records_imported(self):
			return self.__records_checked

		def increment_imported(self):
			self.__records_checked += 1

		def load_start_time(self):
			return self.__load_start_time

		def import_started(self):
			self.__import_start_time = utc_datetime()

		def import_start_time(self):
			return self.__import_start_time

	__import_meta__ = None

	__addrobj_xml_re__ = re.compile('^AS_ADDROBJ_(\d{8})_(.{1,36})\.[X,x][M,m][L,l]$')

	def __init__(self):
		WThreadTask.__init__(self, thread_name='FIAS-Import', join_on_stop=True, ready_to_stop=True,)

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

	def __addrobj_xml(self):
		xml_directory = WAppsGlobals.config['lanbilling-addresses']['fias_directory']
		xml_filename = None
		re_obj = WFIASImportingTask.__addrobj_xml_re__

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

	@classmethod
	def addrobj_mongo_collection(cls):
		connection = WFIASImportingTask.__import_meta__.connection()
		collection = connection['AS_ADDROBJ']
		collection.create_index("AOGUID")
		return collection

	def __load_addrobj(self, entry, mongo_collection):
		WFIASImportingTask.__import_meta__.increment_loaded()
		mongo_collection.insert_one(dict(entry.attrib))
		self.__cleanup_entry(entry)

	@cache_control(storage=WInstanceSingletonCacheStorage(cache_record_cls=WGUIDCacheRecord))
	def cached_ao_level(self, ao_guid, mongo_collection):
		mongo_record = mongo_collection.find_one({'AOGUID': ao_guid})
		return mongo_record['AOLEVEL']

	def __import_addrobj(self, record, mongo_collection, rpc_object):
		ao_level = record['AOLEVEL']
		WLanbillingAddressesImporter.import_address(rpc_object, record, mongo_collection)
		WFIASImportingTask.__import_meta__.increment_imported()

	def __cleanup_entry(self, entry):
		entry.clear()
		while entry.getprevious() is not None:
			del entry.getparent()[0]
		del entry

	def start(self):
		WAppsGlobals.log.info('FIAS import is starting. Loading data')
		if WFIASImportingTask.__import_meta__ is not None:
			WAppsGlobals.log.warn('Multiple FIAS importing tasks spotted - quiting')
			return

		WFIASImportingTask.__import_meta__ = WFIASImportingTask.ImportMeta()

		WAppsGlobals.log.info('Clearing cache')
		WAddressImportCacheSingleton.storage.clear()

		addrobj_mongo_collection = self.addrobj_mongo_collection()
		WAppsGlobals.log.info('Clearing mongo-collection')
		addrobj_mongo_collection.delete_many({})

		WAppsGlobals.log.info('Loading data')
		for entry in self.__addrobj_iterate():
			if self.stop_event().is_set() is True:
				WAppsGlobals.log.warn("Fias loading task terminated. Importing wasn't complete")
				return
			self.__load_addrobj(entry, addrobj_mongo_collection)
		WAppsGlobals.log.info('FIAS loaded successfully. Import is starting')
		WFIASImportingTask.__import_meta__.import_started()

		if self.stop_event().is_set() is True:
			WAppsGlobals.log.warn("Fias import terminated. Importing wasn't complete")

		for record in addrobj_mongo_collection.find():
			if self.stop_event().is_set() is True:
				WAppsGlobals.log.warn("Fias import terminated. Importing wasn't complete")
				return
			self.__import_addrobj(record, addrobj_mongo_collection, self.__rpc_client)

		WAppsGlobals.log.info('FIAS imported successfully')

		WAppsGlobals.log.info('Clearing cache')
		WAddressImportCacheSingleton.storage.clear()

	def stop(self):
		WAppsGlobals.log.info('FIAS import was stopped')
		if WFIASImportingTask.__import_meta__ is not None:
			WFIASImportingTask.__import_meta__.connection().close()
		WFIASImportingTask.__import_meta__ = None
