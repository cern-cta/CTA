#!/bin/sh
#
# process_log_events.sh
#
# Extract timing information for CTA events from CI logs. Create artifacts for analysis and
# visualisation.

CTA_HOME=${HOME}/CTA/continuousintegration
SCRIPT_HOME=${CTA_HOME}/log_analysis/CI

# Location of the latest set of artefacts
ARTEFACTS_HOME=/tmp/pod_logs
ARTEFACT_HOME=${ARTEFACTS_HOME}/$(ls -t ${ARTEFACTS_HOME} | head -1)

# Location of the logfiles we want to analyse
MGM_LOG=${ARTEFACT_HOME}/ctaeos/eos/mgm/xrdlog.mgm
FRONTEND_LOG=${ARTEFACT_HOME}/ctafrontend/cta/cta-frontend.log
TAPED_LOG=${ARTEFACT_HOME}/tpsrv01/taped/cta/cta-taped.log

# The path of the run we want to analyse
EOS_PATH=$(grep "Creating test dir in eos:" ${ARTEFACT_HOME}/systests.sh.log | cut -d' ' -f6)

# Create temporary working directory
#TMPDIR=/tmp/$(basename ${0}).$$
TMPDIR=/tmp/logevents
rm -rf ${TMPDIR}
mkdir -p ${TMPDIR}
cd ${TMPDIR}

# Extract the events of interest from the logfiles
${SCRIPT_HOME}/preprocess.sh ${EOS_PATH} ${MGM_LOG} ${FRONTEND_LOG} ${TAPED_LOG}
${SCRIPT_HOME}/get_times.sh >cta_times_abs.csv
${SCRIPT_HOME}/postprocess.sh cta_times_abs.csv >cta_times_rel.csv
