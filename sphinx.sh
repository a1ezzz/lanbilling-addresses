#!/bin/sh

PACKAGE_DIR=`dirname $0`
cd $PACKAGE_DIR

rm -rf docs/sphinx-html/* && rm -rf docs/sphinx/api/*
sphinx-build docs/sphinx docs/sphinx-html ${SPHINX_BUILD_FLAGS}
