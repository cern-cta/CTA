# Working with Development Pods

Use these commands to inspect a running development instance and investigate failures. Examples use namespace `dev`; substitute the namespace used for your deployment. Replace `<pod>` and `<container>` with the names from your instance.

## Find and inspect pods

List the pods and check their readiness, status, and restart counts:

```bash
kubectl -n dev get pods
```

Inspect a pod's container names, states, restart reasons, and events:

```bash
kubectl -n dev describe pod <pod>
```

A service pod should have all its containers ready. Pods belonging to successfully finished setup jobs can show `Completed`.

## Run commands

Open a shell in a running container:

```bash
kubectl -n dev exec -it <pod> -c <container> -- bash
```

Use `sh` if Bash is unavailable. You can omit `-c <container>` for a pod with a single container; for pods with multiple containers, select the one you intend to inspect.

Run a command directly without opening a shell, for example:

```bash
kubectl -n dev exec cta-cli-0 -- cta-admin version
```

## Read logs

Read logs from a container:

```bash
kubectl -n dev logs <pod> -c <container>
```

Add `--tail=100` to limit the initial output to the last 100 lines. This option is optional.

Follow new log output:

```bash
kubectl -n dev logs <pod> -c <container> -f
```

After a container restarts, inspect the logs from its previous instance, if available:

```bash
kubectl -n dev logs <pod> -c <container> --previous
```

These commands show standard output and standard error. Some services also write log files under `/var/log` inside the container; the exact location depends on the service and its configuration.

For investigating startup failures, crashes, and core dumps, see [Debugging](debugging.md). For more log options, see the [kubectl logs reference](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_logs/).
