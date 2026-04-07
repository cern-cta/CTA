# Presets

This directory contains a number of `values.yaml` files that can be passed to the Helm deployment for local development purposes.

The files should follow the naming convention:

```txt
<release>-<component>[-<type>]?-values.yaml
```

Current conventions are:
- `<release> = dev` refers to the machine of a developer
- `<release> = pipeline` refers to the CI pipeline on GitLab
- `<release> = stress` refers to the stress test configuration
