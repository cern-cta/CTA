# Helm & Orchestration Conventions

Follow these conventions when changing CTA deployment charts and manifests. Keep chart-specific design decisions beside the templates; see also [Helm's best practices](https://docs.helm.sh/docs/chart_best_practices/) and [Container Image Conventions](container-images.md).

## Chart configuration

- Expose deployment-specific settings through documented values rather than hardcoding them in templates. Use values override files for configuration variants instead of duplicating templates.
- Keep defaults minimal and safe for the chart's intended use. Make resource requests and limits configurable.
- Credentials MUST NOT be committed in manifests or values files. Reference externally supplied Secrets or the deployment's secret-management mechanism.
- Use descriptive names and consistent [Kubernetes labels](https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/). Prefer lowercase, hyphen-separated chart and resource names.
- Keep templates simple, reuse common definitions where helpful, and explain non-obvious choices beside the implementation. Prefer native Helm and Kubernetes features over custom scripting.
- Charts SHOULD render manifests whose lifecycle is managed through standard Kubernetes resources. Avoid relying on Helm-specific lifecycle behaviour, such as install/upgrade hooks or cluster lookups during rendering, where practical. This supports deployment through GitOps tools such as Argo CD.

## Workload behaviour

- Use the controller appropriate to the workload, such as a `Deployment`, `StatefulSet`, `DaemonSet`, `Job`, or `CronJob`. Standalone Pods are acceptable for disposable tests.
- Run with least privilege: prefer non-root users, drop unnecessary capabilities, and avoid privilege escalation. Prefer a read-only root filesystem with explicit writable volumes; document any required exceptions, including device or host access.
- Configure probes only where they provide a meaningful health signal. Readiness should reflect the workload's ability to serve its purpose; liveness failures should identify conditions a restart can fix. Use startup probes where needed to allow initialization before other probes run. See [Kubernetes probe guidance](https://kubernetes.io/docs/concepts/workloads/pods/probes/).
- Keep service startup focused on running the daemon. Prefer init containers for prerequisite setup that must finish before the service starts.
- Log to standard output/error by default. When file logging is explicitly needed, such as in tests, configure writable paths, ownership, collection, and retention in the deployment.

## Validation

- Run `helm lint` and inspect `helm template` output with the affected values files. Check that the rendered resources contain the intended settings and references.
- Test deployment changes in a disposable environment, including startup, readiness where applicable, and cleanup. Exercise upgrade behaviour when changing an existing deployment's resources or configuration.
