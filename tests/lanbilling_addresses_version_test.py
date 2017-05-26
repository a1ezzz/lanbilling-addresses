# -*- coding: utf-8 -*-

import pytest
import os
from tempfile import mkdtemp

from lanbilling_addresses.version import revision


@pytest.fixture()
def cwd(request):
	curdir = os.path.curdir

	def fin():
		os.chdir(curdir)
	request.addfinalizer(fin)


@pytest.mark.usefixtures('cwd')
def test_revision(tmpdir):
	revision()
	os.chdir(tmpdir.strpath)
	assert(revision() == '--')
