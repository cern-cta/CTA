// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

const version = process.env.NEW_PIPELINE_IMAGE_VERSION;
if (!/^\d{4}-\d{2}-\d{2}-\d+$/.test(version ?? '')) {
  throw new Error('NEW_PIPELINE_IMAGE_VERSION must identify a completed image build');
}

// Only offer the version from this successful build, even if other pipelines
// have already pushed newer tags for part of the image matrix.
module.exports = {
  ...require('../../renovate.json'),
  requireConfig: 'ignored',
  branchPrefix: `renovate-images/${process.env.CI_COMMIT_REF_SLUG}/`,
  baseBranchPatterns: [process.env.CI_COMMIT_BRANCH],
  enabledManagers: ['custom.regex'],
  packageRules: [{
    matchDatasources: ['docker'],
    allowedVersions: `/^${version}$/`,
  }],
  pinDigests: false,
  customManagers: [{
    customType: 'regex',
    managerFilePatterns: ['/^\\.gitlab-ci\\.yml$/'],
    matchStrings: ['PIPELINE_IMAGE_VERSION: "(?<currentValue>\\d{4}-\\d{2}-\\d{2}-\\d+)"'],
    depNameTemplate: 'cta-pipeline-images',
    packageNameTemplate: 'gitlab-registry.cern.ch/cta/public_registry/container_registry/cta-ci-automation',
    datasourceTemplate: 'docker',
    versioningTemplate: 'regex:^(?<major>\\d{4})-(?<minor>\\d{2})-(?<patch>\\d{2})-(?<build>\\d+)$',
  }],
};
