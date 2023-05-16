#!/bin/sh

# fetch params from meta-data service
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
AZ_ID=$(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone)

# store fqdn hostname in var
HOST=$(hostname -f)


cat <<EOF | tee /tmp/payload
{
  "version": 1,
  "host": "${HOST}",
  "port": 6400,
  "region_id": "{{ console_region_id }}",
  "instance_id": "${INSTANCE_ID}",
  "http_host": "${HOST}",
  "http_port": 9898,
  "active": false,
  "availability_zone_id": "${AZ_ID}"
}
EOF

# check if pageserver already registered or not
if ! curl -sf -H "Authorization: Bearer {{ CONSOLE_API_TOKEN }}" {{ console_mgmt_base_url }}/management/api/v2/pageservers/${INSTANCE_ID} -o /dev/null; then

    # not registered, so register it now
    ID=$(curl -sf -X POST -H "Authorization: Bearer {{ CONSOLE_API_TOKEN }}" -H "Content-Type: application/json" {{ console_mgmt_base_url }}/management/api/v2/pageservers -d@/tmp/payload | jq -r '.id')

    # init pageserver
    sudo -u pageserver /usr/local/bin/pageserver -c "id=${ID}" -c "pg_distrib_dir='/usr/local'" --init -D /storage/pageserver/data
fi
