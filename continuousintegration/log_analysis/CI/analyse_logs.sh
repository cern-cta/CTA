#!/bin/sh
#
# analyse_logs.sh
#
# Extract timing information for CTA events from CI logs. Create artifacts for analysis and
# visualisation.

# Parameters, should be determined from the artifacts
SCRIPTDIR=${HOME}/CTA/continuousintegration/log_analysis/CI
ARTIFACT_HOME=${1-/tmp/pod_logs/archiveretrieve-367256gitec0e2007-a7ki}
TEST_RUN=${2-378e3d54-f89f-4e5d-83ef-e4c78d60b16c}

# Location of the logfiles we want to analyse
MGM_LOG=${ARTIFACT_HOME}/ctaeos/eos/mgm/xrdlog.mgm
FRONTEND_LOG=${ARTIFACT_HOME}/ctafrontend/cta/cta-frontend.log
TAPED_LOG=${ARTIFACT_HOME}/tpsrv01/taped/cta/cta-taped.log

# Create temporary working directory
TMPDIR=/tmp/analyse_logs
rm -rf ${TMPDIR}
mkdir ${TMPDIR}
cd ${TMPDIR}

# Extract the events of interest from the logfiles
${SCRIPTDIR}/preprocess.sh ${TEST_RUN} ${MGM_LOG} ${FRONTEND_LOG} ${TAPED_LOG}

