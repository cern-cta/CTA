{{$namespace := .Release.Namespace }}
{{$name := .Values.postgres.name}}

{{- with .Values.postgres.service}}
apiVersion: v1
kind: Service
metadata:
  name: {{$name}}
  labels:
    k8s-app: {{$name}}
spec:
  selector:
    k8s-app: {{$name}}
  clusterIP: None
  ports:
  {{- .ports | toYaml | nindent 4}}
{{- end}}



{{- with .Values.postgres.pod}}
apiVersion: v1
kind: Pod
metadata:
  name: {{$name}}
  namespace: {{ $namespace | quote}}
  labels:
    k8s-app: {{$name}}
spec:
  restartPolicy: Never
  containers:
  - name: {{$name}}
    image: {{ .image | quote }}
    stdin: true
    env:
    - name: MY_NAME
      value: {{$name}}
    - name: MY_NAMESPACE
      value: {{ $namespace | quote}}
    - name: INSTANCE_NAME
      value: {{ $namespace | quote}}
    {{- if (.env) }}
        {{- .env | toYaml | nindent 4}}
    {{- end}}
    command: {{.command}}
    args: {{.args}}

  imagePullSecrets:
  - name: ctaregsecret
{{- end }}