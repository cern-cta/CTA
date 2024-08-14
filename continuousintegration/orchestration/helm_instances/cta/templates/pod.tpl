
{{$namespace := .Release.Namespace }}
{{$imageVersion := .Values.global.image}}
{{$imageSecret := .Values.global.imagePullSecret}}
{{- range $key, $value := .Values.cta.pods}}

apiVersion: v1
kind: Pod
metadata:
  name: {{$key | quote}}
  namespace: {{ $namespace | quote}}
  labels:
  {{- if ($value.labels)}}
    {{$value.labels | toYaml  | nindent 4}}
  {{- else}}
    k8s-app: {{$key | quote}}
  {{- end}}
spec:
  restartPolicy: Never
  containers:
  {{- range $value.containers}}
    {{$containerName := .name | default $key | quote }}
  - name: {{ $containerName }}
    image: {{ default $imageVersion .image  | quote }}
    {{- if (.commandsAtRuntime)}}
    readinessProbe:
      exec:
        command: {{.commandsAtRuntime | toJson}}
      initialDelaySeconds: {{.delay}}
      periodSeconds: 10
      failureThreshold: {{.failureTolerance}}
    {{- end}}
    stdin: true
    env:
    - name: MY_NAME
      value: {{ $key | quote}}
    - name: MY_NAMESPACE
      value: {{ $namespace | quote}}
    - name: INSTANCE_NAME
      value: {{ $namespace | quote}}
    {{- if (.env) }}
        {{- .env | toYaml | nindent 4}}
    {{- end}}
    command: {{.command | toJson}}
    {{- if (.args)}}
    args: {{.args}}
    {{- end}}
    securityContext:
      privileged: {{.isPriviliged}}
    {{- if (.ports)}}
    ports:
        {{- .ports | toYaml | nindent 4}}
    {{- end}}
    {{- if (.volumeMounts)}}
    volumeMounts:
    {{- .volumeMounts | toYaml | nindent 4}}
    {{- if ($.Values.global.volumeMounts) }}
      {{- $.Values.global.volumeMounts | toYaml | nindent 4}}
    {{- end}}
    {{- end}}
  {{- end}}
  {{- if ($value.volumes)}}
  volumes:
    {{- $value.volumes | toYaml | nindent 2}}
    {{- if ($.Values.global.volumes) }}
      {{- $.Values.global.volumes | toYaml | nindent 2}}
    {{- end}}
  {{- end}}
  imagePullSecrets:
  - name: {{$imageSecret}}
---
{{- end}}