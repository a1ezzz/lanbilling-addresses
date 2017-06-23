# -*- coding: utf-8 -*-
# lanbilling_addresses/importer.py
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

from zeep.exceptions import Fault as SOAPFault

from wasp_general.verify import verify_type, verify_value
from wasp_general.cache import cache_control, WInstanceSingletonCacheStorage, WCacheStorage
from wasp_launcher.apps import WAppsGlobals

from lanbilling_addresses.lanbilling import WLanbillingAddresses, WLanbillingRPC


class WParentGUIDCacheRecord(WInstanceSingletonCacheStorage.InstanceCacheRecord):

	__cache_size__ = 10000

	@verify_value(decorated_function=lambda x: callable(x))
	def __init__(self, result, decorated_function):
		WInstanceSingletonCacheStorage.InstanceCacheRecord.__init__(self, result, decorated_function)
		self.__result = [result]

	@classmethod
	def cache_size(cls):
		return cls.__cache_size__

	def update(self, result, *args, **kwargs):
		mongo_record = args[0].mongo_record()
		if mongo_record is not None:
			if 'PARENTGUID' in mongo_record:
				parent_guid = mongo_record['PARENTGUID']
				cached_value = (parent_guid, result)
				cache_size = self.cache_size()
				if len(self.__result) >= cache_size:
					self.__result = self.__result[:(cache_size - 1)]

				self.__result.insert(0, cached_value)

	@verify_value(decorated_function=lambda x: callable(x))
	def cache_hit(self, decorated_function, *args, **kwargs):
		mongo_record = args[0].mongo_record()
		if mongo_record is not None:
			if 'PARENTGUID' in mongo_record:
				parent_guid = mongo_record['PARENTGUID']

				for i in range(len(self.__result)):
					cache_entry = self.__result[i]
					if cache_entry[0] == parent_guid:
						result = cache_entry[1]
						if i != 0:
							self.__result.insert(0, self.__result.pop(i))
						return WCacheStorage.CacheHit(has_value=True, cached_value=result)
		return WCacheStorage.CacheHit()


class WAddressPartImportAdapter(WLanbillingAddresses.AddressPart):

	__required_ao_level__ = None

	@verify_type(lanbilling_rpc=WLanbillingRPC, fields_map=(dict, None))
	def __init__(self, lanbilling_rpc, fields_map=None, mongo_record=None, mongo_collection=None):
		WLanbillingAddresses.AddressPart.__init__(self, '!', '!')
		self.__fields_map = fields_map
		self.__lanbilling_rpc = lanbilling_rpc
		self.__mongo_record = mongo_record
		self.__mongo_collection = mongo_collection

		if mongo_record is not None and self.__required_ao_level__ is not None:
			if mongo_record['AOLEVEL'] not in self.__required_ao_level__:
				print('ERROR')
				print(mongo_record)
				raise RuntimeError('Invalid AO Level: ' + mongo_record['AOLEVEL'])

	def fields_map(self):
		return self.__fields_map

	def lanbilling_rpc(self):
		return self.__lanbilling_rpc

	def mongo_record(self):
		return self.__mongo_record

	def mongo_collection(self):
		return self.__mongo_collection

	@cache_control(storage=WInstanceSingletonCacheStorage())
	def rpc_record(self):
		result = self._map_fields()
		result.update(self._map_indexes())
		return result

	def _map_fields(self):
		result = {}
		fields_map = self.fields_map()
		if fields_map is not None:
			record = self.mongo_record()
			if record is not None:
				for key, value in fields_map.items():
					result[key] = record[value]
		return result

	def _map_indexes(self):
		return {}

	@verify_type(value=(int, None))
	def mongo_recordid(self, value=None):
		record = self.mongo_record()
		if '__recordid' in record:
			return record['__recordid']
		if value is not None:
			mongo_collection = self.mongo_collection()
			if mongo_collection is not None:
				mongo_collection.update_one({'_id': record['_id']}, {'$set': {'__recordid': value}})
				return value
			else:
				raise RuntimeError('No mongo collection was specified')

	@verify_type(force_import=bool)
	def rpc_recordid(self, force_import=False):
		lanbilling_rpc = self.lanbilling_rpc()
		rpc_record = self.rpc_record()
		result = self.get(lanbilling_rpc, **rpc_record)

		def filter_result(result_item):
			for key, value in rpc_record.items():
				if result_item[key] != value:
					return False
			return True

		result = list(filter(filter_result, result))

		if len(result) == 1:
			return result[0]['recordid']
		elif len(result) > 1:
			raise RuntimeError('Multiple entries found')
		elif force_import is True:
			return self.import_record()

	def import_record(self):
		mongo_record = self.mongo_record()
		mongo_collection = self.mongo_collection()

		try:
			if mongo_record is not None and mongo_collection is not None:
				mongo_recordid = self.mongo_recordid()
				if mongo_recordid is not None:
					return mongo_recordid

			recordid = self.rpc_recordid()
			if recordid is None:
				recordid = self.update(self.lanbilling_rpc(), **self.rpc_record())
				if mongo_record is not None and mongo_collection is not None:
					self.mongo_recordid(recordid)
			return recordid
		except SOAPFault as e:
			print('ERROR PROCESSING: ' + str(mongo_record))
			WAppsGlobals.log.error('SOAP fault:\n' + str(e))
		except RuntimeError as e:
			print('ERROR PROCESSING: ' + str(mongo_record))
			WAppsGlobals.log.error('SOAP fault:\n' + str(e))

	def get_parent(self):
		record = self.mongo_record()
		mongo_collection = self.mongo_collection()
		if record is None or mongo_collection is None:
			raise RuntimeError('No mongo record or collection was specified')

		if 'PARENTGUID' in record:
			parent_guid = record['PARENTGUID']
			return mongo_collection.find_one({'AOGUID': parent_guid})

	@cache_control(storage=WInstanceSingletonCacheStorage(cache_record_cls=WParentGUIDCacheRecord))
	def parent_id(self, parent_classes=None):
		parent = self.get_parent()
		adapter = WLanbillingAddressesImporter.get_part(parent)
		if parent_classes is not None and adapter not in parent_classes:
			raise RuntimeError('Invalid parent class')

		adapter = adapter(
			self.lanbilling_rpc(), self.get_parent(), self.mongo_collection()
		)
		return adapter.rpc_recordid(force_import=True)


class WLanbillingAddressesImporter(WLanbillingAddresses):

	class CountryAdapter(WAddressPartImportAdapter, WLanbillingAddresses.Country):

		__country_name__ = 'Российская Федерация'

		@verify_type(lanbilling_rpc=WLanbillingRPC)
		def __init__(self, lanbilling_rpc):
			WAddressPartImportAdapter.__init__(self, lanbilling_rpc)
			WLanbillingAddresses.Country.__init__(self)

		def _map_fields(self):
			return {'name': WLanbillingAddressesImporter.CountryAdapter.__country_name__}

		@staticmethod
		@verify_type(lanbilling_rpc=WLanbillingRPC)
		def country_id(lanbilling_rpc):
			country_adapter = WLanbillingAddressesImporter.CountryAdapter(lanbilling_rpc)
			country_recordid = country_adapter.rpc_recordid(force_import=True)
			return country_recordid

	class RegionAdapter(WAddressPartImportAdapter, WLanbillingAddresses.Region):
		"""
		bugs found:

		- 'FORMALNAME': 'Чувашская Республика -' for AOID '2001460c-9211-4732-85c2-920935b18a7c'
		- FORMALNAME': 'Чувашская Республика -' and 'SHORTNAME': 'Чувашия' for
		AOID '27b89426-1c17-4eb9-8e81-4411c8ecb069'
		- 'FORMALNAME': 'Саха /Якутия/' for AOID 'd9e4c4c3-3dbe-4fc5-ac26-8e9102af5bd9'
		"""

		__required_ao_level__ = ['1']

		@verify_type(lanbilling_rpc=WLanbillingRPC)
		def __init__(self, lanbilling_rpc, mongo_record, mongo_collection):
			WAddressPartImportAdapter.__init__(
				self, lanbilling_rpc, mongo_record=mongo_record, mongo_collection=mongo_collection,
				fields_map={'name': 'FORMALNAME', 'shortname': 'SHORTNAME'}
			)
			WLanbillingAddresses.Region.__init__(self)

		def _map_indexes(self):
			country_id = WLanbillingAddressesImporter.CountryAdapter.country_id(self.lanbilling_rpc())
			if country_id is None:
				raise RuntimeError('Unable to find country id')

			return {'country': country_id}

		@cache_control(storage=WInstanceSingletonCacheStorage())
		def rpc_record(self):
			rpc_record = WAddressPartImportAdapter.rpc_record(self)
			mongo_record = self.mongo_record()
			if mongo_record is not None:
				region_code = mongo_record['REGIONCODE']
				ao_id = mongo_record['AOID']
				if region_code == '21' and ao_id in [
					'27b89426-1c17-4eb9-8e81-4411c8ecb069', '2001460c-9211-4732-85c2-920935b18a7c'
				]:
					rpc_record['name'] = 'Чувашская Республика'
					rpc_record['shortname'] = 'Респ'
				elif region_code == '14' and ao_id == 'd9e4c4c3-3dbe-4fc5-ac26-8e9102af5bd9':
					rpc_record['name'] = 'Саха (Якутия)'
			return rpc_record

	class AreaAdapter(WAddressPartImportAdapter, WLanbillingAddresses.Area):
		""" Original XML file has multiple records with "SHORTNAME" field like 'г.о.', but current lanbilling
		LTS version (19.1) doesn't support that kind of short name. It must be inserted into LBCore database
		in order to import these records and all of theirs children.

		SQL:
		INSERT INTO address_meaning(short, name, level2) VALUES ('г.о.', 'Городской округ', 1);
		"""

		__required_ao_level__ = ['3']

		@verify_type(lanbilling_rpc=WLanbillingRPC)
		def __init__(self, lanbilling_rpc, mongo_record, mongo_collection):
			WAddressPartImportAdapter.__init__(
				self, lanbilling_rpc, mongo_record=mongo_record, mongo_collection=mongo_collection,
				fields_map={'name': 'FORMALNAME', 'shortname': 'SHORTNAME'}
			)
			WLanbillingAddresses.Area.__init__(self)

		def _map_indexes(self):
			region_id = self.parent_id(parent_classes=[WLanbillingAddressesImporter.RegionAdapter])
			if region_id is None:
				raise RuntimeError('Unable to find region id')
			return {'region': region_id}


	@classmethod
	def get_part(cls, mongo_record):

		parts_map = {
			"1": WLanbillingAddressesImporter.RegionAdapter,
			"3": WLanbillingAddressesImporter.AreaAdapter,
			# "4": WLanbillingAddresses.City,
			# "5": WLanbillingAddresses.Settle,
			# "6": WLanbillingAddresses.Settle,
			# "65": WLanbillingAddresses.Settle,
			# "7": WLanbillingAddresses.Street,
			# "90": WLanbillingAddresses.Settle,
			# "91": WLanbillingAddresses.Street
		}

		aolevel = mongo_record['AOLEVEL']
		part = None
		if aolevel in parts_map:
			part = parts_map[aolevel]
		if part is None:
			WAppsGlobals.log.error('Unable to find suitable part for "AOLEVEL" - %s' % aolevel)
			print(mongo_record)
		else:
			part = part
		return part

	@classmethod
	@verify_type(lanbilling_rpc=WLanbillingRPC)
	def import_address(cls, lanbilling_rpc, mongo_record, mongo_collection):
		part = cls.get_part(mongo_record)
		part = part(lanbilling_rpc, mongo_record=mongo_record, mongo_collection=mongo_collection)
		part.import_record()
