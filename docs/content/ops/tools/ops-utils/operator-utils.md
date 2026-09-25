---
title: Operator utilities overview
---

# CTA Operations Utilities

The CTA Operator Utilities are a collection of free and open source operator tools which may be used to administer a 
CTA system. These tools supplement existing CTA tools to allow for easier operator actions, automation and monitoring.

The source code repository of the tools can be found on [Gitlab](https://gitlab.cern.ch/cta/cta-operations-utilities)

## Contents

The CTA Operations Utilities repository contains a set of Python command line tools, as well as configuration examples for various monitoring technologies.

```
.
├── ci_helpers                         <-- Misc. CI utilities
├── cta-ops-config.yaml                <-- Reference config file
├── monitoring
│   ├── grafana
│   │   └── dashboards                 <-- Dashboard json and previews
│   ├── fluentd                        <-- Fluentd config files examples
│   └── rsyslog                        <-- Rsyslog config file examples
├── LICENSE
├── README.md
├── requirements.txt                   <-- Full install requirements list
└── tools
    └── pip                            <-- Individual tools, written in Python
        ├── ctautils                   <-- Misc. utilities
        ├── tapeadmin                  <-- Tape interaction utilities
        ├── atresys                    <-- Repack automation tools
        ├── ctaopsadmin                <-- CTA admin operation tools
        ├── poolsupply                 <-- Tape pool media refill automation
        ├── tapeverify                 <-- Data health check automation
        ├── tapealerting               <-- Local and bird's-eye-view tape drive and media alerting
        ├── ctaopsdrvconfgen           <-- Drive naming and configuration based on library data
        ├── ctaopsdrvenv               <-- Monitor drive temperature and humidity
        └── ctaopseos                  <-- Tools for interacting with metadata found in EOS
```

## Installation

The tools may be installed using pip. We recommend to create a dedicated virtual environment, in order to avoid dependency conflicts with other pip packages.

For example:

``` bash
git clone https://gitlab.cern.ch/cta/cta-operations-utilities.git
# Review the cta-operations-utilities/requirements.txt file to select packages to install, or leave as-is to install everything.
python3 -m venv venv
source venv/bin/activate
python3 -m pip install --extra-index-url https://cta-public-repo.web.cern.ch/cta-operations/pip/simple/ --requirement cta-operations-utilities/requirements.txt
```

### Requirements

Requirements, such as other pip packages and back-end databases, are specified by each tool.
In general, you will need Python version 3.6.5 or later to run these.

### Configuration

You will have to provide a configuration file for the tools to use, which by default is expected to be found at `/etc/cta-ops/cta-ops-config.yaml`.
A reference config file is provided at the root of this repository: `cta-ops-config.yaml`
The `global` contains a set of general config options and default shared by all tools.
Each tool has it's own section under `tools` with tools-specific settings and overrides of the defaults.
You will only need the sections corresponding to the tools you install.

Additionally, some of the command line tools and library functionalities depend on knowing some basic information about the locally installed tape drives.
This information should be provided in a dedicated json file on the tape server, which is pointed to by the `global::tape::drive_facts` config option.
A command line tool to generate such a file from the information provided by the tape libraries themselves is included in the `ctaopsdrvconfgen` package.
The documentation for this package also specifies the intended format, in case you prefer to create this file manually.

## Building

Use the master Makefile at the top of the project to build all tools, or build tools individually using the provided tool-level Makefile.

The build process relies on Python's `build` [package](https://build.pypa.io/en/stable/) and `setuptools`.

## Documentation

The tools in this repository are documented here and the same information can also be found in their respective README files.

## Compatibility and Support

The provided tools are developed to support tape libraries in the IBM TS4500 and Spectra Logic TFinity families, and tape media in the IBM3592 and LTO families together with their corresponding drives.
Other tape libraries and drives/media have likely not been tested, but we welcome any contribution to extend support to these as well.

These tools are shared "as-is".
Monitoring DB contents are meant to be ephemeral, and you must be willing to wipe them when needed during an upgrade.
We will not provide upgrade paths.

However, please do discuss these tools and report problems on the [CTA Community forum](https://cta-community.web.cern.ch/)

## Conventions

* Operator tools are written in Python, so each tool is published as a pip package.
* Operations command line tools are named `cta-ops-<domain>-<action>'. An example: `cta-ops-repack-manager`.
    * Allows operators to easily find available CTA commands by typing `cta-<tab>`.
    * Avoids name collisions with future core CTA tools.
* The default branch of the repo may be unstable, we ask users to use the latest non-dev tag.
* Each tool should have its own dedicated wiki page, which may be the same as the tool's README.
* Tools should get their config from a single cta-ops yaml config file. A reference file is included in this repo.
* The repo tags have the format `x.y`, where the major version is incremented when CTA changes in a backwards-incompatible way. `y` will be incremented as new versions of the operations tools are released, and reset when switching to a new minimum CTA version.

## License

The code for all tools here are shared under the GPLv3 license.
Note that external dependencies may have other licenses.

## Troubleshooting

### Trouble installing psycopg2

Some CTA operator tools depend on a PostgreSQL database to function, however, psycopg2 can be a bit tricky to [install](https://www.psycopg.org/docs/install.html).
If the usual install and the binary install fail, you can try to build it yourself:

``` bash
python3 -m pip install wheel
tar xf psycopg2-*.tar.gz && pushd psycopg2-*/ && python3 -m pip wheel --wheel-dir ../ . && popd
```
Then install the package and re-run the install of the CTA operator tools as usual.

## Other useful resources

- [IBM tape-automation](https://github.com/IBM/tape-automation) - Open source tape tools by IBM, such as ITDT
