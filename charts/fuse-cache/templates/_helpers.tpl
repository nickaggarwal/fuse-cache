{{- define "fuse-cache.namespace" -}}
{{- .Values.namespace.name -}}
{{- end -}}

{{- define "fuse-cache.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fuse-cache.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "fuse-cache.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Reject cross-cluster configurations the coordinator would refuse, or would
accept and then do something other than what the values say.

Each of these is a silent failure if it reaches the cluster: a missing
clusterId leaves a cluster unidentifiable, an embedded control plane next to
a TLS secret quietly ignores the secret, and global-read on a client whose
coordinator never joined simply never fires. A rendering error names the
problem; a running pod does not.
*/}}
{{- define "fuse-cache.validateGlobal" -}}
{{- $g := .Values.globalCP -}}
{{- if $g.enabled -}}
  {{- if not $g.clusterId -}}
    {{- fail "fuse-cache: globalCP.enabled requires globalCP.clusterId -- this cluster's name in the global namespace. Under mTLS it must match the first DNS SAN of globalCP.tls.secretName's certificate." -}}
  {{- end -}}
  {{- if and (not $g.endpoint) (not $g.embedded) -}}
    {{- fail "fuse-cache: globalCP.enabled requires globalCP.endpoint (host:port of the control plane's gRPC port), unless globalCP.embedded=true." -}}
  {{- end -}}
  {{- if and $g.embedded $g.tls.secretName -}}
    {{- fail "fuse-cache: globalCP.embedded and globalCP.tls.secretName are mutually exclusive. The embedded control plane is loopback-only and serves no certificate, so the secret would be mounted and never used." -}}
  {{- end -}}
  {{- if and (not $g.publishPrefixes) (not $g.subscribePrefixes) -}}
    {{- fail "fuse-cache: globalCP.enabled with neither publishPrefixes nor subscribePrefixes joins the federation and then does nothing -- nothing is offered and nothing is discovered. Set at least one." -}}
  {{- end -}}
{{- else -}}
  {{- if $g.read -}}
    {{- fail "fuse-cache: globalCP.read requires globalCP.enabled. The client asks its own coordinator for cross-cluster locations, and a coordinator that has not joined the federation has none to give." -}}
  {{- end -}}
{{- end -}}
{{- end -}}
