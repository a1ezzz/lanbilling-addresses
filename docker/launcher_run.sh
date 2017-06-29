#!/bin/bash

WASP_LAUNCHER_CONFIGURATION=`tempfile -p wasp_launcher_ -d .`

cat << EOF > "$WASP_LAUNCHER_CONFIGURATION"

[wasp-launcher::applications]
guest_applications_modules =
	wasp_launcher.guest_apps,
	lanbilling_addresses

guest_applications =
	com.binblob.wasp-launcher.guest-apps.wasp-basic,
	com.binblob.wasp-launcher.guest-apps.broker-commands,
	com.binblob.lanbilling-addresses.fias-import,
	com.binblob.lanbilling-addresses.cron-app.fias-import,
	com.binblob.lanbilling-addresses.broker-commands

host_applications =
	com.binblob.wasp-launcher.host-app.broker::start

[wasp-launcher::scheduler::cron]
com.binblob.lanbilling-addresses.cron-app.fias-import = * * * * *

[lanbilling-addresses]
mongo_connection = mongodb://localhost:27017/
mongo_database = lanbilling-addresses
compact_records = True

fias_directory = /fias-data
EOF

echo "login=$LABILLING_LOGIN" >> "$WASP_LAUNCHER_CONFIGURATION"
echo "password=$LABILLING_PASSWORD" >> "$WASP_LAUNCHER_CONFIGURATION"
echo "hostname=$LABILLING_HOSTNAME" >> "$WASP_LAUNCHER_CONFIGURATION"
echo "wsdl_url=$LABILLING_WSDL_URL" >> "$WASP_LAUNCHER_CONFIGURATION"
echo "soap_proxy_address=$LABILLING_SOAP_PROXY_ADDRESS" >> "$WASP_LAUNCHER_CONFIGURATION"

WASP_VERIFIER_DISABLE_CHECKS=paranoid WASP_LAUNCHER_CONFIG_FILE="$WASP_LAUNCHER_CONFIGURATION" wasp-launcher.py

rm "$WASP_LAUNCHER_CONFIGURATION"
