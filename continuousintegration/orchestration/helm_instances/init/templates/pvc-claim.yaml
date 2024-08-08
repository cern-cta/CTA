{{$namespace := .Release.Namespace }}
{{- range $key, $value := .Values.pvcs}}
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: {{$key | quote}}
  namespace: {{ $namespace | quote }}
spec:
  accessModes:
    {{- $value.accessModes | toYaml | nindent 4}}
  resources:
    requests:
      storage: {{ $value.storage }}
  selector:
    matchLabels:
      {{- $value.selectors | toYaml | nindent 6}}
---
{{- end}}
