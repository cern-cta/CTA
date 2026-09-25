---
title: CI orchestration conventions
---

# Orchestration Conventions

All conventions below apply to any usage of Kubernetes and/or Helm.

- Work that can be done during container image build time MUST NOT be deferred to runtime (e.g., installing packages).
    - During testing, packages MAY be installed at runtime if this simplifies the setup.
- Pods MUST be defined via higher-level controllers (`Deployment`, `StatefulSet`, `DaemonSet`) and NOT as standalone Pods in production.
    - Standalone Pods MAY be used for testing only.
- Pods MUST be configured with least privilege:
    - `securityContext.runAsNonRoot: true` MUST be set unless root is explicitly required.
    - Only required Linux capabilities MAY be added; all others MUST be dropped.
    - `readOnlyRootFilesystem: true` MUST be used unless write access is required.
- Sensitive data MUST be stored in Kubernetes Secrets and referenced securely in Pods.
    - Secrets MUST NOT be hardcoded in manifests.
- Probes MUST be configured:
    - `readinessProbe` MUST indicate when the container is ready to receive traffic.
    - `livenessProbe` MUST indicate when the container should be restarted.
    - `startupProbe` MUST be used for applications with long initialization times.
- Containers MUST log to `stdout` and `stderr`.
- Resource `requests` and `limits` MUST be configurable through the `values.yaml` file.
- Helm templates MUST NOT hardcode values that are reasonably expected to change. Such values MUST come from `values.yaml`.
- Configuration variants SHOULD be handled via alternative `values.yaml` files rather than duplicating ConfigMaps, unless duplication is strictly necessary.
- Init logic SHOULD be placed in `initContainers` rather than the main container entrypoint.
- Application entrypoints SHOULD be as simple as possible (ideally starting only the main service).
- Labels and annotations SHOULD follow the [Kubernetes recommended labels](https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/) and project-specific conventions.
- Communication between Pods SHOULD be restricted using NetworkPolicies where applicable.
- Helm `values.schema.json` SHOULD be provided to validate configuration values.
- The default `values.yaml` SHOULD be minimal, safe for production defaults, and avoid unnecessary complexity.
- Each resource SHOULD be defined in its own file; if tightly coupled, group them in a dedicated directory.

---

## Helm Chart Structure

Each chart MUST adhere to the following directory structure:

```
my-chart/
├── charts/                # Directory for dependent subcharts
├── templates/             # YAML templates for Kubernetes resources
│   ├── _helpers.tpl       # Helper templates (e.g., for names, labels)
│   ├── deployment.yaml    # Deployment manifest (optional)
│   ├── service.yaml       # Service manifest (optional)
│   └── ingress.yaml       # Ingress manifest (optional)
├── Chart.yaml             # Chart metadata (name, version, etc.)
├── values.yaml            # Default configuration values
├── values.schema.json     # JSON schema for validating values (optional)
├── README.md              # Documentation
```

## Naming Conventions

- Names MUST use lowercase letters.
- Hyphens (`-`) MUST be used as separators.
- Names MUST be descriptive but concise.
- Only alphanumeric characters and hyphens are allowed.

**Patterns:**

- Chart Directory: `<name>/`
    - Example: `my-app/`, `cta/`
- Values File: `values.yaml` (default values file)
- Custom Values Files: `<env>[-<resource>]-values.yaml`
    - Example: `dev-values.yaml`, `prod-values.yaml`, `ci-catalogue-oracle-values.yaml`
- Templates Directory: `templates/`

**Resource Templates:**

- One resource per file unless tightly coupled; in that case, use a subdirectory.
- Standard names for common types:
    - Pod: `pod.yaml` (testing only)
    - Deployment: `deployment.yaml`
    - Service: `service.yaml`
    - Ingress: `ingress.yaml`
- Descriptive names + type suffix for other resources:
    - ConfigMap: `<name>-configmap.yaml`
    - Secret: `<name>-secret.yaml`
    - Job: `<name>-job.yaml`
    - CronJob: `<name>-cronjob.yaml`
- If resources are grouped in a dedicated directory (`configmaps/`, `secrets/`), use `<name>.yaml` instead of suffixing.

!!! tip

    - Prefer features provided natively by Kubernetes or Helm before adding scripting logic.
    - Use semantic and descriptive file names for resource templates (e.g., `deployment.yaml`, `service.yaml`, `<name>-configmap.yaml`).
    - Add comments in Helm templates for non-obvious logic.
