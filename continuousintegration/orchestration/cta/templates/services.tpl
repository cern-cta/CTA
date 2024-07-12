{{$namespace := .Release.Namespace}}
{{- range $serviceName, $service := .Values.cta.services}}
apiVersion: v1
kind: Service
metadata:
  name: {{$serviceName}}
  namespace: {{$namespace}}
  labels:
    {{$service.labels | toYaml | nindent 4}}
spec:
  selector:
    {{$service.selectors | toYaml | nindent 4}}
  clusterIP: None
  ports:
    {{$service.ports | toYaml | nindent 4}}
{{- end}}