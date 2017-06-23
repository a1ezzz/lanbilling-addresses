# -*- coding: utf-8 -*-
# lanbilling_addresses/lanbilling.py
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

from zeep import Client as SOAPClient
from wasp_general.verify import verify_type, verify_value


class WLanbillingRPC:

	@verify_type(hostname=(str, None), login=(str, None), password=(str, None), wsdl_url=(str, None))
	@verify_type(soap_proxy=bool, soap_proxy_service=(str, None), soap_proxy_address=(str, None))
	@verify_value(hostname=lambda x: x is None or len(x) > 0, login=lambda x: x is None or len(x) > 0)
	@verify_value(wsdl_url=lambda x: x is None or len(x) > 0, soap_proxy_service=lambda x: x is None or len(x) > 0)
	@verify_value(soap_proxy_address=lambda x: x is None or len(x) > 0)
	def __init__(
		self, hostname=None, login=None, password=None, wsdl_url=None, soap_proxy=False,
		soap_proxy_service=None, soap_proxy_address=None
	):
		default = lambda x, d: x if x is not None else d

		self.__hostname = default(hostname, 'localhost')
		self.__login = default(login, 'admin')
		self.__password = default(password, '')
		self.__wsdl_url = default(wsdl_url, ('http://%s/admin/soap/api3.wsdl' % hostname))

		self.__soap_proxy_address = None
		self.__soap_proxy_service = None
		if soap_proxy is True:
			self.__soap_proxy_address = default(soap_proxy_address, ('http://%s:34012' % hostname))
			self.__soap_proxy_service = default(soap_proxy_service, '{urn:api3}api3')

		self.__client = None
		self.__service = None

	def hostname(self):
		return self.__hostname

	def login(self):
		return self.__login

	def password(self):
		return self.__password

	def wsdl_url(self):
		return self.__wsdl_url

	def proxy_address(self):
		return self.__soap_proxy_address

	def proxy_service(self):
		return self.__soap_proxy_service

	def soap_client(self):
		return self.__client

	def soap_proxy(self):
		return self.__service

	def connect(self):
		self.close()
		self.__client = SOAPClient(self.__wsdl_url)
		proxy_address = self.proxy_address()
		proxy_service = self.proxy_service()

		if proxy_address is not None and proxy_service is not None:
			self.__service = self.__client.create_service(proxy_service, proxy_address)
		self._rpc().Login(self.__login, self.__password)

	def _rpc(self):
		if self.__service is not None:
			return self.__service
		if self.__client is not None:
			return self.__client.service
		raise RuntimeError('RPC call before connect')

	def close(self):
		if self.__client is not None:
			self._rpc().Logout()
		self.__client = None
		self.__service = None

	def rpc(self):
		if self.__client is None:
			self.connect()
		return self._rpc()


class WLanbillingAddresses:

	class AddressPart:

		@verify_type(update_method=str, get_method=str)
		@verify_value(update_method=lambda x: len(x) > 0, get_method=lambda x: len(x) > 0)
		def __init__(self, update_method, get_method):
			self.__update_method = update_method
			self.__get_method = get_method

		def update_method(self):
			return self.__update_method

		def get_method(self):
			return self.__get_method

		@verify_type(lanbilling_rpc=WLanbillingRPC, recordid=(int, None))
		def update(self, lanbilling_rpc, **fields):
			method = getattr(lanbilling_rpc.rpc(), self.update_method())
			args = {'recordid': 0}
			args.update(fields)
			return method(0, args)

		@verify_type(lanbilling_rpc=WLanbillingRPC, recordid=(int, None))
		def get(self, lanbilling_rpc, **fields):
			method = getattr(lanbilling_rpc.rpc(), self.get_method())
			args = {}
			args.update(fields)
			return method(args)

	class Country(AddressPart):

		def __init__(self):
			WLanbillingAddresses.AddressPart.__init__(self, 'insupdAddressCountry', 'getAddressCountries')

		@verify_type(lanbilling_rpc=WLanbillingRPC, recordid=(int, None), name=str)
		def update(self, lanbilling_rpc, name=None, **fields):
			return WLanbillingAddresses.AddressPart.update(self, lanbilling_rpc, name=name, **fields)


	class Region(AddressPart):

		def __init__(self):
			WLanbillingAddresses.AddressPart.__init__(self, 'insupdAddressRegion', 'getAddressRegions')

		@verify_type(lanbilling_rpc=WLanbillingRPC, recordid=(int, None), country=int, name=str, shortname=str)
		def update(self, lanbilling_rpc, country=None, name=None, shortname=None, **fields):
			return WLanbillingAddresses.AddressPart.update(
				self, lanbilling_rpc, country=country, name=name, shortname=shortname, **fields
			)

	class Area(AddressPart):

		def __init__(self):
			WLanbillingAddresses.AddressPart.__init__(self, 'insupdAddressArea', 'getAddressAreas')

		@verify_type(lanbilling_rpc=WLanbillingRPC, recordid=(int, None), region=int, name=str, shortname=str)
		def update(self, lanbilling_rpc, region=None, name=None, shortname=None, **fields):
			return WLanbillingAddresses.AddressPart.update(
				self, lanbilling_rpc, region=region, name=name, shortname=shortname, **fields
			)

	class City(AddressPart):
		"""
		SOAP-object:

		recordid - long
		region - long
		area - long
		name - str
		shortname - str
		"""

		def __init__(self):
			WLanbillingAddresses.AddressPart.__init__(self, 'insupdAddressCity', 'getAddressCities')


	class Settle(AddressPart):
		"""
		SOAP-object:

		recordid - long
		region - long
		area - long
		city - long
		name - str
		shortname - str
		"""

		def __init__(self):
			WLanbillingAddresses.AddressPart.__init__(self, 'insupdAddressSettle', 'getAddressSettles')

	class Street(AddressPart):
		"""
		SOAP-object

		recordid - long
		region - long
		city - long
		settl - long
		idx - long (index)
		name - str
		shortname - str
		"""

		def __init__(self):
			WLanbillingAddresses.AddressPart.__init__(self, 'insupdAddressStreet', 'getAddressStreets')
