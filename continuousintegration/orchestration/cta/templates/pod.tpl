
{{$namespace := .Release.Namespace }}
{{$imageVersion := .Values.global.image}}

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
    image: {{ $imageVersion | quote }}
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
    command: {{.command}}
    args: {{.args}}
    securityContext:
      privileged: {{.isPriviliged}}
    {{- if (.ports)}}
    ports:
        {{- .ports | toYaml | nindent 4}}
    {{- end}}
    volumeMounts:
    {{- .volumeMounts | toYaml | nindent 4}}
  {{- end}}
  volumes:
    {{- $value.volumes | toYaml | nindent 2}}

  imagePullSecrets:
  - name: ctaregsecret
---
{{- end}}