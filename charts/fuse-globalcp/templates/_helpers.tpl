{{- define "fuse-globalcp.namespace" -}}
{{- .Values.namespace.name -}}
{{- end -}}

{{- define "fuse-globalcp.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fuse-globalcp.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "fuse-globalcp.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Refuse to render a configuration the binary would refuse to start on.

Without mTLS the control plane cannot tell who is calling, so it exits
rather than serve a port where any caller may publish as any cluster
(cmd/globalcp/main.go). Catching that here turns a CrashLoopBackOff an
operator has to read logs to explain into a failed `helm install` that says
why -- the same fail-closed rule, moved one layer earlier.
*/}}
{{- define "fuse-globalcp.validate" -}}
{{- if and (not .Values.tls.secretName) (not .Values.allowInsecureIdentity) -}}
{{- fail "fuse-globalcp: set tls.secretName (mTLS), or allowInsecureIdentity=true for a dev install. Without mTLS the control plane cannot verify which cluster is calling, so it refuses to start: any caller could publish as any cluster." -}}
{{- end -}}
{{- if and .Values.tls.secretName .Values.allowInsecureIdentity -}}
{{- fail "fuse-globalcp: tls.secretName and allowInsecureIdentity are mutually exclusive. Setting both reads as if certificates were optional; they are required by the transport once TLS is on." -}}
{{- end -}}
{{- if and (gt (int .Values.replicaCount) 1) (not .Values.allowMultiReplica) -}}
{{- fail "fuse-globalcp: replicaCount > 1 with the in-memory store gives each cluster a different view of the index depending on which pod its stream lands on. Wire the etcd-backed store first, or set allowMultiReplica=true if you know this install is single-tenant." -}}
{{- end -}}
{{- end -}}
