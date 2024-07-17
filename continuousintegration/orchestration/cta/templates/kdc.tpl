#It is as seperate file as its not really a part of the app
#So if the need to put as subchart arises it will be simpler to move
{{$namespace := .Release.Namespace }}
{{$accountName := .Values.kdc.serviceAccount.name }}
{{$name := .Values.kdc.name}}
{{- with .Values.kdc.service}}
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

---

{{- with .Values.kdc.pod}}
apiVersion: v1
kind: Pod
metadata:
  name: {{$name}}
  namespace: {{ $namespace | quote}}
  labels:
    k8s-app: {{$name}}
spec:
  serviceAccountName: {{$accountName}}
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
    securityContext:
      privileged: {{.isPriviliged}}
    volumeMounts:
    {{- .volumeMounts | toYaml | nindent 4}}

  volumes:
    {{- .volumes | toYaml | nindent 2}}

  imagePullSecrets:
  - name: ctaregsecret
{{- end }}

---

{{- with .Values.kdc.serviceAccount}}

kind: Role
apiVersion: rbac.authorization.k8s.io/v1
metadata:
 name: {{ printf "%s-%s" .name "role" }}
rules:
    {{- .rules | toYaml | nindent 2}}

---

apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{.name}}
  namespace: {{$namespace}}

---

apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ printf "%s-%s" .name "roleBinding" }}
  namespace: {{$namespace}}
subjects:
- kind: ServiceAccount
  name: {{.name}}
roleRef:
  kind: Role
  name: {{ printf "%s-%s" .name "role" }}
  apiGroup: rbac.authorization.k8s.io
{{- end}}