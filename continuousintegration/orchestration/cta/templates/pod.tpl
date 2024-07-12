
{{$namespace := .Release.Namespace }}
{{$imageVersion := .Values.global.image}}

{{- range $key, $value := .Values.cta.pods}}

apiVersion: v1
kind: Pod
metadata:
  name: {{$key | quote}}
  namespace: {{ $namespace | quote}}
  labels:
    k8s-app: {{$key | quote}}
spec:
  restartPolicy: Never
  containers:
  - name: {{ $key | quote}}
    image: {{ $imageVersion | quote }}
    stdin: true
    env:
    - name: MY_NAME
      value: {{ $key | quote}}
    - name: MY_NAMESPACE
      value: {{ $namespace | quote}}
    - name: INSTANCE_NAME
      value: {{ $namespace | quote}}
    {{- if ($value.env) }}
        {{- $value.env | toYaml | nindent 4}}
    {{- end}}
    command: {{$value.command}}
    args: {{$value.args}}
    securityContext:
      privileged: {{$value.isPriviliged}}
    {{- if ($value.ports)}}
    ports:
        {{- $value.ports | toYaml | nindent 4}}
    {{- end}}
    volumeMounts:
    {{- $value.volumeMounts | toYaml | nindent 4}}
  
  volumes:
    {{- $value.volumes | toYaml | nindent 2}}

  imagePullSecrets:
  - name: ctaregsecret
---
{{- end}}