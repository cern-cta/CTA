!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Debugging

Start with the state of the development instance, then narrow the failure to a service or request. Examples use namespace `dev`; substitute your deployment's namespace and the relevant pod and container names. See [Working with Development Pods](development-pods.md) for shell and log commands.

## Check that the instance started correctly

```bash
kubectl -n dev get pods
kubectl -n dev describe pod <pod>
```

Check that service containers are ready and whether restart counts are increasing. Successfully finished setup jobs can show `Completed`. A pod being `Running` does not by itself mean the application is ready.

Replace `<pod>` with a name from `kubectl get pods`. For `<container>`, use a container name listed by `kubectl describe pod` under **Containers** or **Init Containers**.

| Symptom | What to check |
| --- | --- |
| `Pending` | Run `kubectl -n dev describe pod <pod>`. In **Events**, check for unavailable resources, node constraints, or unbound volumes. If the pod is already assigned to a node, check container startup and volume-mount errors. |
| `ErrImagePull` or `ImagePullBackOff` | Run `kubectl -n dev describe pod <pod>`. Check the **Image** field and pull errors in **Events**. Verify the image name and tag; authentication errors can indicate missing or outdated registry credentials. For local images, see [image-loading troubleshooting](cta-dev.md#troubleshooting). |
| `CrashLoopBackOff` or repeated restarts | Run `kubectl -n dev describe pod <pod>`, then `kubectl -n dev logs <pod> -c <container> --previous`. Under the affected container, check **Last State**, **Reason**, and **Exit Code**, then read the logs from its previous instance. |
| `Running` but not ready | Run `kubectl -n dev describe pod <pod>`, then `kubectl -n dev logs <pod> -c <container>`. Check for readiness-probe failures in **Events** and corresponding errors in the application logs. |

## Inspect the logs

Read the affected container's logs around the time of the failure. If it restarted, also check the previous container's logs using `--previous`; see [Read logs](development-pods.md#read-logs).

Look for the first error and follow the request across the services involved, using request identifiers, file IDs, and timestamps where available. Check dependency logs when an error points to the catalogue, scheduler, or disk system. Some processes also write files under `/var/log`; check their configured log locations.

For CI failures, download the available logs and diagnostic artifacts before reproducing locally. See [Investigating CI failures](../testing/ci/pipelines.md#investigating-ci-failures).

## Check for core dumps

The CTA development charts mount the core-dump directory at `/var/log/tmp`. Check it after a suspected crash, including when the service has recovered and the pod now appears healthy:

```bash
kubectl -n dev exec <pod> -c <container> -- find /var/log/tmp -type f -name '*.core'
```

This requires a running container. If the container cannot stay running, first inspect its termination reason and previous logs. A missing core dump does not rule out a crash; whether one is written depends on the termination cause and core-dump configuration.

Preserve relevant dumps and logs before replacing the deployment. The default core-dump volume is ephemeral and is lost when the pod is deleted. For a running container, copy a dump to your machine without requiring a debugger in the service image:

```bash
kubectl -n dev exec <pod> -c <container> -- cat /var/log/tmp/<core-file> > ./<core-file>
```

Record the image tag or digest, source revision, and configuration alongside the dump. Analysing it requires the matching executable, libraries, and debug symbols. See [Debugger tooling](#debugger-tooling) for the planned core-dump analysis instructions (currently TODO).

## Reproduce a failing operation

Once the instance is healthy enough to run the operation, narrow the failure to a repeatable example. For example, if a test suite fails during retrieval, rerun the relevant [system test](../testing/system-tests.md) instead of the entire suite, keeping the same scheduler backend and disk system.

Record the steps, expected result, and observed failure. Check logs and core dumps immediately afterwards to connect the failed operation to the affected service. A repeatable example helps you investigate the cause and later verify the fix. Preserve the original logs, dumps, and configuration before rebuilding or redeploying.

## Debugger tooling

TODO: Document the revised `cta-dev debug` workflow for general debugging and core-dump analysis, including obtaining matching binaries and debug symbols, building on the approach used by `ci/ci-debug.sh`.
