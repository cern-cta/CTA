{{- /*
SPDX-FileCopyrightText: 2025 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{- define "ctafrontend.fullname" -}}
  {{ include "common.names.fullname" . }}
{{- end }}

{{- define "ctafrontendgrpc.fullname" -}}
  {{ printf "%s-grpc" (include "common.names.fullname" .) }}
{{- end }}

{{- define "ctafrontend.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "ctafrontend.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "ctafrontend.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "scheduler.config" -}}
  {{- $schedulerConfig := .Values.global.configuration.scheduler }}
  {{- if ne (typeOf $schedulerConfig) "string" -}}
    {{- $schedulerConfig = $schedulerConfig | toYaml }}
  {{- end }}
  {{- $schedulerConfig -}}
{{- end }}

{{- define "ctafrontend.grpcStatefulSet" -}}
{{- $root := .root }}
{{- $role := default "wfe" .role }}
{{- $name := printf "%s-%s" (include "ctafrontendgrpc.fullname" $root) $role }}
{{- $schedulerConfig := include "scheduler.config" $root | fromYaml }}
{{- $configMapName := printf "cta-frontend-grpc-%s-conf" $role }}
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {{ $name }}
  namespace: {{ $root.Release.Namespace | quote }}
  labels:
    {{- include "common.labels.standard" $root | nindent 4 }}
spec:
  serviceName: {{ $name }}
  replicas: {{ $root.Values.replicas }}
  selector:
    matchLabels:
      app.kubernetes.io/name: {{ $name }}
  podManagementPolicy: Parallel
  template:
    metadata:
      labels:
        app.kubernetes.io/name: {{ $name }}
        {{- include "common.labels.withoutname" $root | nindent 8 }}
    spec:
      imagePullSecrets:
      {{ include "ctafrontend.imagePullSecrets" $root | nindent 6 }}
      {{- if $root.Values.pod.extraSpec }}
      {{ $root.Values.pod.extraSpec | toYaml | nindent 6 }}
      {{- end }}
      initContainers:
      {{- if and (eq $role "admin") $root.Values.grpc.statefulSet.admin.enableKrb5 }}
      - name: init1-get-krb5-keytab
        image: {{ include "ctafrontend.image" $root }}
        imagePullPolicy: {{ include "ctafrontend.imagePullPolicy" $root }}
        command: ["/bin/bash", "-c"]
        args:
        - |
          sukadmin() { echo {{ $root.Values.global.kerberos.adminPrinc.password }} | kadmin -r {{ $root.Values.global.kerberos.defaultRealm | upper }} -p {{ $root.Values.global.kerberos.adminPrinc.name }}/admin $@; };
          sukadmin addprinc -randkey cta/{{ $name }};
          sukadmin ktadd -k /root/ctafrontend_keytab/output/cta-frontend.krb5.keytab cta/{{ $name }};
          chmod 400 /root/ctafrontend_keytab/output/cta-frontend.krb5.keytab
          # Make CTA user owner (group 33 is tape)
          chown -R 1000:33 /root/ctafrontend_keytab/output/cta-frontend.krb5.keytab
        volumeMounts:
        - name: krb5-conf
          mountPath: /etc/krb5.conf
          subPath: krb5.conf
        - name: cta-frontend-krb5-keytab
          mountPath: /root/ctafrontend_keytab/output
      {{- end }}
      - name: init2-download-grpc-health-probe
        image: {{ include "common.images.busybox" $root }}
        command: ["sh", "-c"]
        args:
          - |
            mkdir -p /grpc-health-probe && \
            wget -qO/grpc-health-probe/grpc-health-probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.13/grpc_health_probe-linux-amd64 && \
            chmod +x /grpc-health-probe/grpc-health-probe
        volumeMounts:
          - name: grpc-health-probe-bin
            mountPath: /grpc-health-probe
            subPath: grpc-health-probe
      containers:
      - name: cta-frontend
        image: {{ include "ctafrontend.image" $root }}
        imagePullPolicy: {{ include "ctafrontend.imagePullPolicy" $root }}
        stdin: true
        command: ["/bin/bash", "-c"]
        args: ["{{- if $root.Values.preRun.enabled }}/scripts/pre_run.sh;{{- end }} /scripts/ctafrontend-grpc.sh; {{- if $root.Values.postRun.enabled }}/scripts/post_run.sh;{{- end }}"]
        startupProbe:
          exec:
            command: ["rpm", "-q", "cta-frontend-grpc"]
          initialDelaySeconds: 0
          periodSeconds: 5
          failureThreshold: 60
        readinessProbe:
          exec:
            command:
              - /grpc-health-probe/grpc-health-probe
              - -addr=localhost:{{ $root.Values.grpc.port }}
            {{- if $root.Values.grpc.conf.tls.enabled }}
              - -tls
              - -tls-ca-cert=/etc/grpc-certs/ca/ca.crt.pem
              - -tls-server-name={{ printf "cta-frontend-grpc-%s" $role }}
              {{- if and (eq $role "wfe") (eq $root.Values.grpc.conf.auth.wfe "mtls") }}
              - -tls-client-cert=/etc/grpc-certs/client/ctaeos-self-signed.crt.pem
              - -tls-client-key=/etc/grpc-certs/client/ctaeos-self-signed.key.pem
              {{- end }}
            {{- end }}
          initialDelaySeconds: 5
          periodSeconds: 5
          timeoutSeconds: 3
          successThreshold: 1
          failureThreshold: 60
        securityContext:
          allowPrivilegeEscalation: {{ include "common.securityContext.allowPrivilegeEscalation" $root }}
          privileged: {{ include "common.securityContext.privileged" $root }}
        env:
        - name: MY_NAME
          value: {{ $name }}
        - name: MY_NAMESPACE
          value: {{ $root.Release.Namespace | quote }}
        - name: INSTANCE_NAME
          value: {{ $root.Release.Namespace | quote }}
        - name: SCHEDULER_BACKEND
          value: {{ $schedulerConfig.backend | quote }}
        {{- if eq $schedulerConfig.backend "ceph" }}
        - name: SCHEDULER_CEPH_NAMESPACE
          value: {{ $schedulerConfig.cephConfig.namespace | quote }}
        - name: SCHEDULER_CEPH_ID
          value: {{ $schedulerConfig.cephConfig.id | quote }}
        - name: SCHEDULER_CEPH_POOL
          value: {{ $schedulerConfig.cephConfig.pool | quote }}
        {{- end }}
        - name: TERM
          value: "xterm"
        {{- with $root.Values.extraEnv }}
        {{- include "common.extraEnv" . | nindent 8 }}
        {{- end }}
        volumeMounts:
        - name: scripts-volume
          mountPath: /scripts
        - name: catalogue-config
          mountPath: /etc/cta/cta-catalogue.conf
          subPath: cta-catalogue.conf
        {{- if eq $schedulerConfig.backend "postgres" }}
        - name: scheduler-config
          mountPath: /etc/cta/cta-scheduler.conf
          subPath: cta-scheduler.conf
        {{- else }}
        - name: scheduler-config
          mountPath: /etc/cta/cta-objectstore-tools.conf
          subPath: cta-objectstore-tools.conf
        {{- end }}
        {{- if eq $schedulerConfig.backend "ceph" }}
        - name: cta-ceph-volume
          mountPath: /etc/ceph/ceph.conf
          subPath: ceph.conf
        - name: cta-ceph-volume
          mountPath: /etc/ceph/ceph.client.{{ $schedulerConfig.cephConfig.id }}.keyring
          subPath: ceph.client.{{ $schedulerConfig.cephConfig.id }}.keyring
        {{- end }}
        {{- if eq $schedulerConfig.backend "vfs" }}
        - name: vfs-storage-volume
          mountPath: /objectstore
        {{- end }}
        - name: grpc-config
          mountPath: /etc/cta/cta-frontend-grpc.conf
          subPath: cta-frontend-grpc.conf
        {{- if and (eq $role "wfe") (eq $root.Values.grpc.conf.auth.wfe "mtls") }}
        - name: grpc-config
          mountPath: /etc/cta/mtls-map.toml
          subPath: mtls-map.toml
        {{- end }}
        - name: jwks-json
          mountPath: /etc/cta/jwks.json
          subPath: jwks.json
        - name: grpc-server-crt
          mountPath: /etc/grpc-certs/cert
        - name: grpc-server-key
          mountPath: /etc/grpc-certs/key
        - name: grpc-ca-crt
          mountPath: /etc/grpc-certs/ca
        {{- if and (eq $role "wfe") (eq $root.Values.grpc.conf.auth.wfe "mtls") }}
        - name: grpc-client-crt
          mountPath: /etc/grpc-certs/client/ctaeos-self-signed.crt.pem
          subPath: ctaeos-self-signed.crt.pem
        - name: grpc-client-key
          mountPath: /etc/grpc-certs/client/ctaeos-self-signed.key.pem
          subPath: ctaeos-self-signed.key.pem
        {{- end }}
        {{- if and (eq $role "admin") $root.Values.grpc.statefulSet.admin.enableKrb5 }}
        - name: cta-frontend-krb5-keytab
          mountPath: /etc/cta/cta-frontend.krb5.keytab
          subPath: cta-frontend.krb5.keytab
        - name: krb5-conf
          mountPath: /etc/krb5.conf
          subPath: krb5.conf
        {{- end }}
        - name: grpc-health-probe-bin
          mountPath: /grpc-health-probe
          subPath: grpc-health-probe
        {{- if $root.Values.grpc.conf.telemetry.metrics.otlp.auth.basic.passwordSecret }}
        - name: otlp-basic-auth-pwd
          mountPath: /etc/cta/otlp-basic-auth-pwd.conf
          subPath: otlp-basic-auth-pwd.conf
        {{- end }}
        {{- include "common.extraVolumeMounts" $root | nindent 8 }}
      volumes:
      - name: scripts-volume
        configMap:
          name: scripts-ctafrontend
          defaultMode: 0777
      - name: catalogue-config
        configMap:
          name: cta-catalogue-conf
      - name: jwks-json
        secret:
          secretName: jwks-json
      {{- if eq $schedulerConfig.backend "postgres" }}
      - name: scheduler-config
        configMap:
          name: cta-scheduler-conf
      {{- else }}
      - name: scheduler-config
        configMap:
          name: cta-objectstore-conf
      {{- end }}
      {{- if eq $schedulerConfig.backend "ceph" }}
      - name: cta-ceph-volume
        configMap:
          name: ceph-conf
      {{- end }}
      {{- if eq $schedulerConfig.backend "vfs" }}
      - name: vfs-storage-volume
        persistentVolumeClaim:
          claimName: scheduler-vfs-pvc
      {{- end }}
      - name: grpc-config
        configMap:
          name: {{ $configMapName }}
      - name: grpc-server-crt
        secret:
          secretName: {{ printf "server-%s-crt-pem" $role }}
      - name: grpc-server-key
        secret:
          secretName: {{ printf "server-%s-key-pem" $role }}
      - name: grpc-ca-crt
        secret:
          secretName: ca-crt-pem
      {{- if and (eq $role "wfe") (eq $root.Values.grpc.conf.auth.wfe "mtls") }}
      - name: grpc-client-crt
        secret:
          secretName: ctaeos-self-signed-crt-pem
      - name: grpc-client-key
        secret:
          secretName: ctaeos-self-signed-key-pem
      {{- end }}
      {{- if and (eq $role "admin") $root.Values.grpc.statefulSet.admin.enableKrb5 }}
      - name: cta-frontend-krb5-keytab
        emptyDir: {}
      - name: eos-sss-keytab-fixedownership
        emptyDir: {}
      - name: krb5-conf
        configMap:
          name: {{ $root.Values.global.kerberos.clientConfig.configMap }}
      {{- end }}
      - name: grpc-health-probe-bin
        emptyDir: {}
      {{- if $root.Values.grpc.conf.telemetry.metrics.otlp.auth.basic.passwordSecret }}
      - name: otlp-basic-auth-pwd
        secret:
          secretName: {{ $root.Values.grpc.conf.telemetry.metrics.otlp.auth.basic.passwordSecret }}
      {{- end }}
      {{ include "common.extraVolumes" $root | nindent 6 }}
{{- end }}
