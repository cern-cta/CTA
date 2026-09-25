!!! warning "WIP"
    This tool is still experimental

# cta-ops-eos

cta-ops-eos tools are operations tools that interact with EOS. The tools currently included are:

- `cta-ops-get-path-from-eos`
- `cta-ops-change-storageclass`

## Installation

### Configuration

The tool can be configured in the standard config file like so:

```yaml
tools:
  cta-ops-eos:
    cta-ops-get-path-from-eos:
      # The path to the key tab file for making rest requests to EOS
      # In the key tab an instance is a key with a port and a token as values
      rest_key_tab: "/etc/cta/eos.rest.keytab.yaml"
    cta-ops-change-storage-class:
      nb_retries: 10
      # The number of files per execution of cta-change-storage-class-in-catalogue,
      # a higher number leads to more memory usage
      files_per_split: 1000
```

## Usage

### cta-ops-get-path-from-eos

This tool fetches the EOS namespace path for a specified file.
It utilizes the EOS REST API, and in order to enable requests to an instance, the user must have an EOS token.
This should either be stored in the default `eos.rest.keytab.yaml` location (`/etc/cta/eos.rest.keytab.yaml`) or in the file with a path specified in the `cta-ops-config.yaml` file.
You can find examples of configuration files (`cta-ops-config.yaml` and `tools/pip/ctaopseos/eos.rest.keytab.yaml`) in `the cta-operations-utilities` repository.

To generate an EOS token:

``` bash
eos space config default space.token.generation={GENERATION_NUMBER}
TOKEN=$(eos token --tree --path '{EOS_PATH}' --expires $LATER  --owner admin1)
```

Place this token in the `eos.rest.keytab.yaml` on the client side.

### cta-ops-change-storageclass

This tool may be used to bulk-change the storageclass of files which already exist both on EOS and CTA in an idempotent fashion.
To use it, first create an EOS dump for the path in question, such that each line contains the json representation of a file:

``` bash
eos-ns-inspect scan --json --path <eos_path> --members <qdb_host_name>:<qdb_port> --password-file <password_file> | jq -c '.[]' > dump_file.json
```

Then run the tool:

``` bash
cta-ops-change-storage-class --help
usage: cta-ops-change-storage-class [-h] -l LOCATION_OF_DUMP -i INSTANCE
                                    [-n NB_FILES_PER_EXECUTION]
                                    [-C CONFIG_FILE] [-V VERBOSE]

Change storage class for a given path in EOS.

optional arguments:
  -h, --help            show this help message and exit
  -l LOCATION_OF_DUMP, --location-of-dump LOCATION_OF_DUMP
                        The location of dump
  -i INSTANCE, --instance INSTANCE
                        The instance where the files are located in EOS
  -n NB_FILES_PER_EXECUTION, --nb-files-per-execution NB_FILES_PER_EXECUTION
                        The number of files per split for the input files,
                        higher number will lead to more memory usage

settings:
  Configure how this script runs

  -C CONFIG_FILE, --config-file CONFIG_FILE
                        The path for the config file (file has to be in YAML
                        format)
  -V VERBOSE, --verbose VERBOSE
                        Verbose logging
```
