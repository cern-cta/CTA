[![Latest Release](https://gitlab.cern.ch/cta/CTA/-/badges/release.svg)](https://gitlab.cern.ch/cta/CTA/-/releases)
[![License](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](COPYING)
[![Code Quality](https://sonarcloud.io/api/project_badges/measure?project=cern-cta_CTA&metric=alert_status)](https://sonarcloud.io/project/overview?id=cern-cta_CTA)
[![Last Commit](https://img.shields.io/gitlab/last-commit/cta%2FCTA?gitlab_url=https%3A%2F%2Fgitlab.cern.ch)](https://gitlab.cern.ch/cta/CTA/-/commits/main?ref_type=heads)

<br />
<div align="center">
  <a href="https://gitlab.cern.ch/cta/CTA/">
    <img src="assets/cta-logo.png" alt="Logo">
  </a>
<h3 align="center" style="padding-top: 0">CTA</h3>
  <p align="center">
    The CERN Tape Archive open source tape data management software.
    <br />
    <a href="https://eoscta.docs.cern.ch/"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://eoscta.docs.cern.ch/latest/overview/getting_started/">Getting Started</a>
    ·
    <a href="https://cta.web.cern.ch/">CTA Website</a>
    ·
    <a href="https://cta-community.web.cern.ch/">Community Forum</a>
  </p>
</div>

___

## About CTA

The CERN Tape Archive (CTA) is the storage system for the custodial copy of all physics data at CERN. CTA was designed to cope with the huge storage and performance demands of [LHC Run-3](https://home.cern/press/2022/run-3) and beyond.

CTA is:

- The leading Free and Open Source tape data management software
- Home to all the experimental physics data at CERN
- Scalable, for small and large installations
- Self-hostable, on-premise

## Getting Started

The simplest way to get start with CTA is by using the publicly-available RPMs.

### Install CTA

1. Setup the cta-public repo for CTA:

    ```bash
    echo -e "[cta-public-5-alma9]\n \
              name=CTA artifacts\n \
              baseurl=https://cta-public-repo.web.cern.ch/stable/cta-5/el9/cta/x86_64/\n \
              gpgcheck=0\n \
              ena bled=1\n \
              priority=2" \
      > /etc/yum.repos.d/cta-public-5.repo
    ```

2. Install the `cta-release` package:

    ```bash
    yum install cta-release
    ```

3. For a very minimal install, you need the following packages.

    On the CTA frontend:

    ```bash
    yum install cta-frontend cta-catalogueutils
    ```

    On the CTA tape servers:

    ```bash
    yum install cta-taped cta-tape-label
    ```

For more detailed instructions and configuration, please visit [the documentation](https://eoscta.docs.cern.ch/latest/overview/getting_started/).

## License

> This program is free software, distributed under the terms of the GNU General Public Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can redistribute it and/or modify it under the terms of the GPL Version 3, or (at your option) any later version.
>
> This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
>
> In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an Intergovernmental Organization or submit itself to any jurisdiction.

See the [COPYING](COPYING) file for additional details.

___

<div align="center">
  <a href="https://home.cern/">
    <img src="assets/cern-logo.png" alt="CERN Logo" width="128" height="128">
  </a>
</div>
