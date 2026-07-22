#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Setup environment
./ci/cta-dev.sh install
cta-dev up --scheduler-type pgsched --spawn-options "--one-logical-library --max-drives 2"
cta-dev deploy --scheduler-type pgsched --spawn-options "--one-logical-library --max-drives 2"

# Run setup script
source ci/mount_candidate_demo/setup.sh --namespace dev

# Due to the setup.sh script relaunching the maintd routines, we need to manually remove the lease instead of waiting 300 seconds
# TODO: Have a way to release the lease when the pod routine responsible for refreshing the mount candidates is redeployed
# TODO: Then this will no longer be necessary
kubectl -n dev exec cta-scheduler-postgres-db -- psql -U cta -d cta-scheduler-postgres -c "delete from scheduler_mount_work_lock where work_key = 'mount-candidate-refresh';"

# Just to validate that the correct pod has now the lease
# TODO: To be fixed and removed
kubectl -n dev get pods
kubectl -n dev exec cta-scheduler-postgres-db -- psql -U cta -d cta-scheduler-postgres -c "select extract(epoch from current_timestamp)::integer as now, * from scheduler_mount_work_lock;"

# Archive 4 files in different tapes
echo "Archiving file 0..."
xrdcp /etc/group root://ctaeos//eos/ctaeos/cta/example0
until vids=$(cta-admin --json ta ls --all | jq -er '[.[] | select(.state == "ACTIVE" and ((.lastFseq // 0) | tonumber) > 0) | .vid] | select(length > 0) | .[]'); do sleep 0.2; done; printf '%s\n' "$vids" | xargs -r -I {} cta-admin ta ch --state DISABLED --reason "Test" --vid {}
echo "Archiving file 1..."
xrdcp /etc/group root://ctaeos//eos/ctaeos/cta/example1
until vids=$(cta-admin --json ta ls --all | jq -er '[.[] | select(.state == "ACTIVE" and ((.lastFseq // 0) | tonumber) > 0) | .vid] | select(length > 0) | .[]'); do sleep 0.2; done; printf '%s\n' "$vids" | xargs -r -I {} cta-admin ta ch --state DISABLED --reason "Test" --vid {}
echo "Archiving file 2..."
xrdcp /etc/group root://ctaeos//eos/ctaeos/cta/example2
until vids=$(cta-admin --json ta ls --all | jq -er '[.[] | select(.state == "ACTIVE" and ((.lastFseq // 0) | tonumber) > 0) | .vid] | select(length > 0) | .[]'); do sleep 0.2; done; printf '%s\n' "$vids" | xargs -r -I {} cta-admin ta ch --state DISABLED --reason "Test" --vid {}
echo "Archiving file 3..."
xrdcp /etc/group root://ctaeos//eos/ctaeos/cta/example3
until vids=$(cta-admin --json ta ls --all | jq -er '[.[] | select(.state == "ACTIVE" and ((.lastFseq // 0) | tonumber) > 0) | .vid] | select(length > 0) | .[]'); do sleep 0.2; done; printf '%s\n' "$vids" | xargs -r -I {} cta-admin ta ch --state DISABLED --reason "Test" --vid {}

# Check tapes
cta-admin tape ls --all

# Put all drives down
cta-admin --json dr ls | jq -r '.[].driveName' | xargs -I {} cta-admin dr down {} --reason "Test"
cta-admin dr ls

# Try to retrieve all files
echo "Retrieve request for files 0, 1, 2, 3"
xrdfs-retrieve ctaeos prepare -s /eos/ctaeos/cta/example0 /eos/ctaeos/cta/example1 /eos/ctaeos/cta/example2 /eos/ctaeos/cta/example3

# Try to archive two extra files
echo "Archiving file 4 (drives down)"
xrdcp /etc/group root://ctaeos//eos/ctaeos/cta/example4
echo "Archiving file 5 (drives down)"
xrdcp /etc/group root://ctaeos//eos/ctaeos/cta/example5

# Show the mount candidates, with the disabled option
cta-admin mc ls

# Put all tapes up
cta-admin --json ta ls --all | jq -er '.[] | select(.state == "DISABLED") | .vid' | xargs -r -I {} cta-admin ta ch --state ACTIVE --vid {}

# Show some stuff
eos root://ctaeos ls -la /eos/ctaeos/cta/
cta-admin tape ls --all
cta-admin sq
cta-admin mc ls

# Bump up the score of a mount and show again
cta-admin mc ch --ck RETRIEVE-ULT102 --score 100
cta-admin mc ch --ck RETRIEVE-ULT103 --score 101
cta-admin mc ch --ck RETRIEVE-ULT104 --score 102
cta-admin mc ls

# Reset one just to show
cta-admin mc ch --ck RETRIEVE-ULT104 --score 0
cta-admin mc ls

# Put drive up and visualize mounts on repeat...
cta-admin --json dr ls | jq -r '.[].driveName' | head -1 | xargs -I {} cta-admin dr up {}
while true; do
  echo
  echo "================================================================================"
  echo
  cta-admin dr ls
  echo "--------------------------------------------------------------------------------"
  cta-admin mc ls
  if cta-admin --json mc ls | jq -e 'length == 0' >/dev/null; then
    break
  fi
  sleep 0.25
done
