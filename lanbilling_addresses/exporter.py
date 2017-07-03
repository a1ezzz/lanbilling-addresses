# -*- coding: utf-8 -*-
# lanbilling_addresses/exporter.py
#
# Copyright (C) 2017 the lanbilling-addresses authors and contributors
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

import decorator
from zeep.exceptions import Fault as SOAPFault

from wasp_general.verify import verify_type, verify_value
from wasp_general.cache import cache_control, WInstanceSingletonCacheStorage, WCacheStorage
from wasp_launcher.apps import WAppsGlobals

from lanbilling_addresses.lanbilling import WLanbillingAddresses, WLanbillingRPC


class WGUIDCacheRecord(WInstanceSingletonCacheStorage.InstanceCacheRecord):

	__cache_size__ = 50

	@verify_value('paranoid', decorated_function=lambda x: callable(x))
	def __init__(self, result, decorated_function):
		WInstanceSingletonCacheStorage.InstanceCacheRecord.__init__(self, result, decorated_function)
		self.__result = [result]

	@classmethod
	def cache_size(cls):
		return cls.__cache_size__

	def update(self, result, *args, **kwargs):
		guid = args[1]
		cached_value = (guid, result)
		cache_size = self.cache_size()
		if len(self.__result) >= cache_size:
			self.__result = self.__result[:(cache_size - 1)]

		self.__result.insert(0, cached_value)

	def cache_entry(self, *args, **kwargs):
		guid = args[1]
		for i in range(len(self.__result)):
			cache_entry = self.__result[i]
			if cache_entry[0] == guid:
				result = cache_entry[1]
				if i != 0:
					self.__result.insert(0, self.__result.pop(i))
				return WCacheStorage.CacheEntry(has_value=True, cached_value=result)
		return WCacheStorage.CacheEntry()


class WRPCRecordCacheStorage(WCacheStorage):

	__cache_size__ = 1000

	def __init__(self):
		WCacheStorage.__init__(self)
		self.__storage = []
		self.__cache_missed = 0
		self.__cache_hit = 0

	@verify_value(decorated_function=lambda x: callable(x))
	def put(self, result, decorated_function, *args, **kwargs):
		cls = args[0]
		get_method = cls.__get_method__
		fields = dict(kwargs)

		if 'recordid' in fields:
			fields.pop('recordid')

		cached_value = (get_method, fields, result)
		cache_size = self.cache_size()
		if len(self.__storage) >= cache_size:
			self.__storage = self.__storage[:(cache_size - 1)]
		self.__storage.insert(0, cached_value)

	@verify_value(decorated_function=lambda x: callable(x))
	def get_cache(self, decorated_function, *args, **kwargs):
		cls = args[0]
		get_method = cls.__get_method__
		fields = dict(kwargs)

		if 'recordid' in fields:
			fields.pop('recordid')

		for i in range(len(self.__storage)):
			cache_entry = self.__storage[i]
			if cache_entry[0] == get_method and cache_entry[1] == fields:
				result = cache_entry[2]
				if i != 0:
					self.__storage.insert(0, self.__storage.pop(i))
				self.__cache_hit += 1
				return WCacheStorage.CacheEntry(has_value=True, cached_value=result)

		self.__cache_missed += 1
		return WCacheStorage.CacheEntry()

	@verify_value(decorated_function=lambda x: x is None or callable(x))
	def clear(self, decorated_function=None):
		self.__storage.clear()
		self.__cache_missed = 0
		self.__cache_hit = 0

	def cache_missed(self):
		return self.__cache_missed

	def cache_hit(self):
		return self.__cache_hit

	@classmethod
	def cache_size(cls):
		return cls.__cache_size__


class WAddressExportCacheSingleton:
	# THIS STORAGE IS NOT THREAD SAFE!
	guid_cache = WInstanceSingletonCacheStorage(cache_record_cls=WGUIDCacheRecord, statistic=True)
	# THIS STORAGE IS NOT THREAD SAFE!
	rpc_cache = WRPCRecordCacheStorage()

	@staticmethod
	def update_rpc_cache(decorated_function):
		def first_level_decorator(original_function, cls, lanbilling_rpc, **fields):
			result = original_function(cls, lanbilling_rpc, **fields)

			cache_result = dict(fields)
			cache_result['recordid'] = result

			WAddressExportCacheSingleton.rpc_cache.put(
				[cache_result], cls.get, cls, lanbilling_rpc, **fields
			)

			return result

		return decorator.decorator(first_level_decorator)(decorated_function)


class WAddressPartExportAdapter(WLanbillingAddresses.AddressPart):

	__required_ao_level__ = None

	@verify_type(lanbilling_rpc=WLanbillingRPC, fields_map=(dict, None))
	def __init__(self, lanbilling_rpc, fields_map=None, mongo_record=None, mongo_collection=None):
		WLanbillingAddresses.AddressPart.__init__(self)
		self.__fields_map = fields_map
		self.__lanbilling_rpc = lanbilling_rpc
		self.__mongo_record = mongo_record
		self.__mongo_collection = mongo_collection

		if mongo_record is not None and self.__required_ao_level__ is not None:
			if mongo_record['AOLEVEL'] not in self.__required_ao_level__:
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
					if value in record:
						result[key] = record[value]
		return result

	def _map_indexes(self):
		return {}

	@verify_type(value=(int, None))
	def mongo_recordid(self, value=None):
		record = self.mongo_record()
		if '__recordid' in record:
			return record['__recordid']
		if value is not None and ('__recordid' not in record or record['__recordid'] != value):
			mongo_collection = self.mongo_collection()
			if mongo_collection is not None:
				mongo_collection.update_one({'_id': record['_id']}, {'$set': {'__recordid': value}})
				return value
			else:
				raise RuntimeError('No mongo collection was specified')

	def rpc_recordid(self):
		lanbilling_rpc = self.lanbilling_rpc()
		rpc_record = self.rpc_record()
		result = self.get(lanbilling_rpc, **rpc_record)

		if len(result) == 1:
			return result[0]['recordid']
		elif len(result) > 1:
			raise RuntimeError('Multiple entries found')
		return self.update(lanbilling_rpc, **rpc_record)

	def export_record(self):
		mongo_record = self.mongo_record()
		mongo_collection = self.mongo_collection()

		try:
			if mongo_record is not None and mongo_collection is not None:
				mongo_recordid = self.mongo_recordid()
				if mongo_recordid is not None:
					return mongo_recordid

			recordid = self.rpc_recordid()
			if mongo_record is not None and mongo_collection is not None:
				self.mongo_recordid(recordid)
			return recordid
		except SOAPFault as e:
			print('ERROR PROCESSING: %s %s' % (mongo_record['AOLEVEL'], mongo_record['SHORTNAME']))
			print(mongo_record)
			WAppsGlobals.log.error('SOAP fault:\n' + str(e))

	@classmethod
	@cache_control(storage=WAddressExportCacheSingleton.guid_cache)
	def cached_id(cls, ao_guid, lanbilling_rpc, mongo_collection, adapter_classes=None):
		mongo_record = mongo_collection.find_one({'AOGUID': ao_guid})
		adapter_cls = WLanbillingAddressesExporter.get_part(mongo_record)

		if adapter_cls is None:
			raise RuntimeError('No suitable adapter found')

		if adapter_classes is not None and adapter_cls not in adapter_classes:
			raise RuntimeError('Invalid parent class')

		adapter = adapter_cls(
			lanbilling_rpc, mongo_record, mongo_collection
		)
		return adapter.rpc_recordid(), adapter

	@classmethod
	@cache_control(storage=WAddressExportCacheSingleton.rpc_cache)
	def get(cls, lanbilling_rpc, **fields):
		method = getattr(lanbilling_rpc.rpc(), cls.__get_method__)
		args = {}
		args.update(fields)
		result = method(args)

		def filter_result(result_item):
			for key, value in fields.items():
				if key not in result_item:
					return False
				if result_item[key] != value:
					return False
			return True

		return list(filter(filter_result, result))

	@classmethod
	@WAddressExportCacheSingleton.update_rpc_cache
	def update(cls, lanbilling_rpc, **fields):
		method = getattr(lanbilling_rpc.rpc(), cls.__update_method__)
		args = {'recordid': 0}
		args.update(fields)
		return method(0, args)

	def parent_indexes(self):
		region = 0
		area = 0
		city = 0
		settle = 0

		mongo_record = self.mongo_record()
		lanbilling_rpc = self.lanbilling_rpc()
		mongo_collection = self.mongo_collection()

		if mongo_record is not None and 'PARENTGUID' in mongo_record:
			parent_recordid, parent_adapter = self.cached_id(
				mongo_record['PARENTGUID'],
				lanbilling_rpc,
				mongo_collection
			)

			recurse_region, recurse_area, recurse_city, recurse_settle = parent_adapter.parent_indexes()

			if isinstance(parent_adapter, WLanbillingAddressesExporter.RegionAdapter) is True:
				region = parent_recordid
			elif isinstance(parent_adapter, WLanbillingAddressesExporter.AreaAdapter) is True:
				region = recurse_region
				area = parent_recordid
			elif isinstance(parent_adapter, WLanbillingAddressesExporter.CityAdapter) is True:
				region = recurse_region
				area = recurse_area
				city = parent_recordid
			elif isinstance(parent_adapter, WLanbillingAddressesExporter.SettleAdapter) is True:
				region = recurse_region
				area = recurse_area
				city = recurse_city
				settle = parent_recordid
			elif parent_adapter is not None:
				return parent_adapter.parent_indexes()
			else:
				raise RuntimeError('Unable to find suitable adapter: ' + str(parent_adapter))

		return region, area, city, settle


class WLanbillingAddressesExporter(WLanbillingAddresses):

	class CountryAdapter(WAddressPartExportAdapter, WLanbillingAddresses.Country):

		__country_name__ = 'Российская Федерация'

		@verify_type('paranoid', lanbilling_rpc=WLanbillingRPC)
		def __init__(self, lanbilling_rpc):
			WAddressPartExportAdapter.__init__(self, lanbilling_rpc)
			WLanbillingAddresses.Country.__init__(self)

		def _map_fields(self):
			return {'name': WLanbillingAddressesExporter.CountryAdapter.__country_name__}

		@staticmethod
		@verify_type('paranoid', lanbilling_rpc=WLanbillingRPC)
		def country_id(lanbilling_rpc):
			country_adapter = WLanbillingAddressesExporter.CountryAdapter(lanbilling_rpc)
			country_recordid = country_adapter.rpc_recordid()
			return country_recordid

	class BasicAdapter(WAddressPartExportAdapter):

		@verify_type(shortname_substitions=(dict, None))
		def __init__(self, lanbilling_rpc, mongo_record, mongo_collection, shortname_substitions=None):
			WAddressPartExportAdapter.__init__(
				self, lanbilling_rpc, mongo_record=mongo_record, mongo_collection=mongo_collection,
				fields_map={'name': 'FORMALNAME', 'shortname': 'SHORTNAME'}
			)
			self.__substitions = shortname_substitions if shortname_substitions is not None else {}

		def _shortname_substitutions(self):
			return self.__substitions

		def _map_fields(self):
			fields = WAddressPartExportAdapter._map_fields(self)
			shortname = fields['shortname']
			substitutions = self._shortname_substitutions()
			if shortname in substitutions:
				fields['shortname'] = substitutions[shortname]
			return fields

	class RegionAdapter(BasicAdapter, WLanbillingAddresses.Region):
		"""
		bugs found:

		- 'FORMALNAME': 'Чувашская Республика -' for AOID '2001460c-9211-4732-85c2-920935b18a7c'
		- FORMALNAME': 'Чувашская Республика -' and 'SHORTNAME': 'Чувашия' for
		AOID '27b89426-1c17-4eb9-8e81-4411c8ecb069'
		- 'FORMALNAME': 'Саха /Якутия/' for AOID 'd9e4c4c3-3dbe-4fc5-ac26-8e9102af5bd9'
		"""

		__required_ao_level__ = ['1']

		@verify_type('paranoid', lanbilling_rpc=WLanbillingRPC)
		def __init__(self, lanbilling_rpc, mongo_record, mongo_collection):
			WLanbillingAddressesExporter.BasicAdapter.__init__(
				self, lanbilling_rpc, mongo_record=mongo_record, mongo_collection=mongo_collection
			)
			WLanbillingAddresses.Region.__init__(self)

		def _map_indexes(self):
			country_id = WLanbillingAddressesExporter.CountryAdapter.country_id(self.lanbilling_rpc())
			if country_id is None:
				raise RuntimeError('Unable to find country id')

			return {'country': country_id}

		def _map_fields(self):
			fields = WLanbillingAddressesExporter.BasicAdapter._map_fields(self)
			mongo_record = self.mongo_record()
			ao_id = mongo_record['AOID']
			if ao_id in [
				'27b89426-1c17-4eb9-8e81-4411c8ecb069', '2001460c-9211-4732-85c2-920935b18a7c'
			]:
				fields['name'] = 'Чувашская Республика'
				fields['shortname'] = 'Респ'
			elif ao_id == 'd9e4c4c3-3dbe-4fc5-ac26-8e9102af5bd9':
				fields['name'] = 'Саха (Якутия)'
			return fields

	class AreaAdapter(BasicAdapter, WLanbillingAddresses.Area):
		""" Original XML file has multiple records with "SHORTNAME" field like 'г.о.', but current lanbilling
		LTS version (19.1) doesn't support that kind of short name. It must be inserted into LBCore database
		in order to export these records and all of theirs children.

		SQL:
		INSERT INTO address_meaning(short, name, level_2) VALUES ('г.о.', 'Городской округ', 1);
		"""

		__required_ao_level__ = ['3']

		@verify_type('paranoid', lanbilling_rpc=WLanbillingRPC)
		def __init__(self, lanbilling_rpc, mongo_record, mongo_collection):
			WLanbillingAddressesExporter.BasicAdapter.__init__(
				self, lanbilling_rpc, mongo_record=mongo_record, mongo_collection=mongo_collection
			)
			WLanbillingAddresses.Area.__init__(self)

		def _map_indexes(self):
			parent_region, parent_area, parent_city, parent_settle = self.parent_indexes()
			return {'region': parent_region}

	class CityAdapter(BasicAdapter, WLanbillingAddresses.City):
		""" Original XML file has multiple records with "SHORTNAME" field like 'г.', but lanbilling accepts
		 "г" only
		"""

		__required_ao_level__ = ['4']

		@verify_type('paranoid', lanbilling_rpc=WLanbillingRPC)
		def __init__(self, lanbilling_rpc, mongo_record, mongo_collection):
			WLanbillingAddressesExporter.BasicAdapter.__init__(
				self, lanbilling_rpc, mongo_record=mongo_record, mongo_collection=mongo_collection,
				shortname_substitions={'г.': 'г'}
			)
			WLanbillingAddresses.City.__init__(self)

		def _map_indexes(self):
			parent_region, parent_area, parent_city, parent_settle = self.parent_indexes()
			return {'region': parent_region, 'area': parent_area}

	class SettleAdapter(BasicAdapter, WLanbillingAddresses.Settle):
		""" Original XML file has multiple records with "SHORTNAME" field like:
		 - 'тер.', but lanbilling accepts "тер" only
		 - 'тер. СНТ', but lanbilling accepts "снт" only

		 Original XML file has multiple records with "SHORTNAME" field like 'тер. СПК', but current lanbilling
		LTS version (19.1) doesn't support that kind of short name. It must be inserted into LBCore database
		in order to export these records and all of theirs children.

		SQL:
		"""

		__required_ao_level__ = ['5', '6', '90']

		@verify_type('paranoid', lanbilling_rpc=WLanbillingRPC)
		def __init__(self, lanbilling_rpc, mongo_record, mongo_collection):
			WLanbillingAddressesExporter.BasicAdapter.__init__(
				self, lanbilling_rpc, mongo_record=mongo_record, mongo_collection=mongo_collection,
				shortname_substitions={'тер.': 'тер', 'тер. СНТ': 'снт'}
			)
			WLanbillingAddresses.Settle.__init__(self)

		def _map_indexes(self):
			parent_region, parent_area, parent_city, parent_settle  = self.parent_indexes()
			return {'region': parent_region, 'area': parent_area, 'city': parent_city}

	class StreetAdapter(BasicAdapter, WLanbillingAddresses.Street):
		'''

		SQL:
		#INSERT INTO address_meaning(short, name, level_5) VALUES ('днт', 'Дачное некоммерческое товарищество', 1);
		#INSERT INTO address_meaning(short, name, level_5) VALUES ('тсн', 'Товарищество собственников недвижимости', 1);
		#INSERT INTO address_meaning(short, name, level_5) VALUES ('спк', 'Сельскохозяйственный производственный кооператив', 1);
		#INSERT INTO address_meaning(short, name, level_5) VALUES ('дпк', 'Дачный потребительский кооператив', 1);
		#INSERT INTO address_meaning(short, name, level_5) VALUES ('ряд', 'Ряд', 1);
		#INSERT INTO address_meaning(short, name, level_5) VALUES ('мгстр', 'Магистраль', 1);
		#INSERT INTO address_meaning(short, name, level_5) VALUES ('снп', 'Садоводческое некоммерческое партнерство', 1);
		#INSERT INTO address_meaning(short, name, level_5) VALUES ('онп', 'Огородническое некоммерческое партнерство', 1);
		#INSERT INTO address_meaning(short, name, level_5) VALUES ('сно', 'Садоводческое некоммерческое объединение', 1);
		#INSERT INTO address_meaning(short, name, level_5) VALUES ('оно', 'Огородническое некоммерческое объединение', 1);
		#INSERT INTO address_meaning(short, name, level_5) VALUES ('дно', 'Дачное некоммерческое объединение', 1);
		#INSERT INTO address_meaning(short, name, level_5) VALUES ('месторождение', 'Месторождение', 1);
		#INSERT INTO address_meaning(short, name, level_5) VALUES ('съезд', 'Съезд', 1);
		#INSERT INTO address_meaning(short, name, level_5) VALUES ('усадьба', 'Усадьба', 1);
		'''
		__required_ao_level__ = ['65', '7', '91']

		@verify_type('paranoid', lanbilling_rpc=WLanbillingRPC)
		def __init__(self, lanbilling_rpc, mongo_record, mongo_collection):
			WLanbillingAddressesExporter.BasicAdapter.__init__(
				self, lanbilling_rpc, mongo_record=mongo_record, mongo_collection=mongo_collection,
				shortname_substitions={
					'тер. ДНТ': 'днт',
					'тер. ТСН': 'тсн',
					'пр-д': 'проезд',
					'ал.': 'аллея',
					'пер.': 'пер',
					'ш.': 'ш',
					'туп.': 'туп',
					'пл.': 'пл',
					'наб.': 'наб',
					'тер. СНТ': 'снт',
					'тер. СПК': 'спк',
					'тер. ДПК': 'дпк',
					'тер.': 'тер',
					'тер. ОПК': 'тер',
					'тер. ОНТ': 'тер',
					'тер. ГСК': 'гск',
					'мкр.': 'мкр',
					'тер.ф.х.': 'ф/х',
					'лн.': 'линия',
					'тер. ДНП': 'днп',
					'ул.': 'ул',
					'платф.': 'платф',
					'дор.': 'дор',
					'ст.': 'ст',
					'мгстр.': 'мгстр',
					'к-цо': 'кольцо',
					'пер-д': 'переезд',
					'пр-к': 'просек',
					'пр-ка': 'просек',
					'проул.': 'проулок',
					'рзд.': 'рзд',
					'тер. СНП': 'снп',
					'тер. ОНП': 'онп',
					'тер. ДНО': 'дно',
					'тер. ОНО': 'оно',
					'тер. СНО': 'сно',
					'месторожд': 'месторождение',
					'месторожд.': 'месторождение',
					'м-ко': 'м',
					'ж/р': 'ж/д_рзд',
					'ост-в': 'остров',
					'х.': 'х',
					'стр.': 'стр',
					'г-к': 'городок',
					'с-р': 'сквер',
					'с-к': 'спуск',
					'сзд': 'съезд',
					'сзд.': 'съезд',
					'б-г': 'берег',
					'ззд': 'заезд',
					'ус.': 'усадьба'
				}
			)
			WLanbillingAddresses.Street.__init__(self)

		def _map_indexes(self):
			parent_region, parent_area, parent_city, parent_settle = self.parent_indexes()
			return {'region': parent_region, 'city': parent_city, 'settl': parent_settle}

		def _map_fields(self):
			fields = WLanbillingAddressesExporter.BasicAdapter._map_fields(self)
			mongo_record = self.mongo_record()
			if 'POSTALCODE' in mongo_record:
				fields['idx'] = int(mongo_record['POSTALCODE'])
			else:
				fields['idx'] = 0
			return fields

	@classmethod
	def get_part(cls, mongo_record):

		parts_map = {
			"1": WLanbillingAddressesExporter.RegionAdapter,
			"3": WLanbillingAddressesExporter.AreaAdapter,
			"4": WLanbillingAddressesExporter.CityAdapter,
			"5": WLanbillingAddressesExporter.SettleAdapter,
			"6": WLanbillingAddressesExporter.SettleAdapter,
			"65": WLanbillingAddressesExporter.StreetAdapter,
			"7": WLanbillingAddressesExporter.StreetAdapter,
			"90": WLanbillingAddressesExporter.SettleAdapter,
			"91": WLanbillingAddressesExporter.StreetAdapter,
		}

		aolevel = mongo_record['AOLEVEL']
		part = None
		if aolevel in parts_map:
			part = parts_map[aolevel]
		if part is None:
			raise RuntimeError('Unable to find suitable part for "AOLEVEL" - %s')
		else:
			part = part
		return part

	@classmethod
	@verify_type('paranoid', lanbilling_rpc=WLanbillingRPC)
	def export_address(cls, lanbilling_rpc, mongo_record, mongo_collection):
		part = cls.get_part(mongo_record)
		part = part(lanbilling_rpc, mongo_record=mongo_record, mongo_collection=mongo_collection)
		part.export_record()
