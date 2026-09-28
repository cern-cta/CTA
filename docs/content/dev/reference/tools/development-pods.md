# Working with Development Pods

To see all the pods in a namespace (e.g. `dev`), you can use:

```shell
kubectl get pods -n dev
```

In general, to open a shell on any desired `<pod>`, you can execute the following:

```shell
kubectl exec -it -n dev <pod> -- bash
```

Some pods have multiple containers in them. To specify which container to open the shell in, add the `-c` flag:

```shell
kubectl exec -it -n dev <pod> -c <container> -- bash
```

To execute a command directly without opening a dedicated shell (e.g running `cta-admin version`), you can do the following:

```shell
kubectl exec -it -n dev cta-cli-0 -- cta-admin version
```

Most pods will log directly to `stdout/stderr`. To see this, use the `kubectl logs` command:

```shell
kubectl logs <pod> [-c <container>] -n dev
```

Some of the process produce multiple log files (e.g. the XRootD Frontend or the EOS pods). You can find all of these logs in `/var/log` in the corresponding container.
