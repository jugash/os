# Merging the namespace builder token and Vault registry credentials for KMM with External Secrets Operator

## Context

The Kernel Module Management (KMM) `Module` accepts a single `imageRepoSecret`. Two sets of registry credentials need to go into it:

- **Internal registry token.** OpenShift creates a `builder` service account in every namespace, together with a pull secret named `builder-dockercfg-xxxxx`. It is type `kubernetes.io/dockercfg` (legacy format, no `"auths"` wrapper) and its token is rotated automatically. The name has a random suffix, so we **match it by regex** rather than hard-coding it.
- **External registry credentials.** A `kubernetes.io/dockerconfigjson` Secret already synced into the cluster from Vault.

External Secrets Operator (ESO) reads both through its Kubernetes provider and merges them into one `dockerconfigjson` Secret for the module. Because the builder secret is found by pattern, nothing needs updating when the namespace is created or the secret is regenerated, and OpenShift's token rotation is kept.

Example names used below (replace with your own):

| Item | Name |
|---|---|
| Namespace | `kmm-modules` |
| Builder pull secret (auto-generated) | `builder-dockercfg-xxxxx` |
| Vault-synced registry Secret | `vault-registry-creds` |
| Merged output Secret | `kmm-registry` |

No extra registry RBAC is needed. OpenShift already grants the `builder` service account `system:image-builder` in its namespace, which allows push and pull on the internal registry.

## 1. Create a SecretStore that reads the in-cluster secrets

`dataFrom.find` needs `list` on Secrets, and `resourceNames` cannot restrict `list`. The reader can therefore see every Secret in the namespace (see [Things to check](#things-to-check)).

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: eso-secret-reader
  namespace: kmm-modules
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: eso-secret-reader
  namespace: kmm-modules
rules:
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["get", "list", "watch"]      # list is required for find
  - apiGroups: ["authorization.k8s.io"]
    resources: ["selfsubjectrulesreviews"]
    verbs: ["create"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: eso-secret-reader
  namespace: kmm-modules
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: eso-secret-reader
subjects:
  - kind: ServiceAccount
    name: eso-secret-reader
    namespace: kmm-modules
---
apiVersion: external-secrets.io/v1
kind: SecretStore
metadata:
  name: in-cluster
  namespace: kmm-modules
spec:
  provider:
    kubernetes:
      remoteNamespace: kmm-modules
      server:
        caProvider:
          type: ConfigMap
          name: kube-root-ca.crt
          key: ca.crt
      auth:
        serviceAccount:
          name: eso-secret-reader
```

## 2. Create the ExternalSecret that finds the builder secret and merges it

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: kmm-registry
  namespace: kmm-modules
spec:
  refreshInterval: 1m                       # keep up with the rotating token
  secretStoreRef:
    name: in-cluster
    kind: SecretStore
  dataFrom:
    - find:
        name:
          regexp: "^builder-dockercfg-[a-z0-9]{5}$"   # anchored: one match only
      rewrite:
        - regexp:
            source: "^builder-dockercfg-.*$"
            target: "internal"                        # stable key name for the template
  data:
    - secretKey: external                   # dockerconfigjson synced from Vault
      remoteRef:
        key: vault-registry-creds
        property: .dockerconfigjson
  target:
    name: kmm-registry
    creationPolicy: Owner
    template:
      type: kubernetes.io/dockerconfigjson
      engineVersion: v2
      data:
        .dockerconfigjson: |-
          {{- $internal := index (.internal | fromJson) ".dockercfg" | fromJson -}}
          {{- $external := (.external | fromJson).auths -}}
          {{- dict "auths" (merge (dict) $external $internal) | toJson -}}
```

### How it works

1. `find` matches the builder secret by name pattern. ESO returns it under its own name, random suffix included.
2. `rewrite` renames that key to `internal`, so the template can always reference it the same way.
3. With `find`, the Kubernetes provider returns the whole Secret as a JSON object of its keys. The template parses it once to get `.dockercfg`, then parses that value to get the flat `{registry: creds}` map (legacy format).
4. It unwraps `"auths"` from the Vault secret.
5. It merges the two maps. If both contain the same registry, the Vault entry wins because it comes first in `merge`.
6. It wraps the result back in `"auths"`.

### Check the intermediate output first

Step 3 depends on the shape `find` returns. Before adding the template, apply the ExternalSecret **without** `target.template` and inspect the raw `internal` key:

```bash
oc get secret kmm-registry -n kmm-modules -o jsonpath='{.data.internal}' | base64 -d | jq .
# expected: {".dockercfg": "{\"image-registry.openshift-image-registry.svc:5000\":{...}}", ...}
```

If `internal` already contains the registry map directly (no `.dockercfg` wrapper), simplify the first template line to:

```
{{- $internal := .internal | fromJson -}}
```

### Example final output

```json
{"auths":{
  "image-registry.openshift-image-registry.svc:5000":{"username":"<token>","password":"eyJhbGciOi...","auth":"...","email":"..."},
  "registry.example.com":{"username":"svc-kmm","password":"...","auth":"..."}
}}
```

## 3. Point the KMM Module at the merged Secret

```yaml
apiVersion: kmm.sigs.x-k8s.io/v1beta1
kind: Module
metadata:
  name: my-kmod
  namespace: kmm-modules
spec:
  imageRepoSecret:
    name: kmm-registry
  moduleLoader:
    container:
      modprobe:
        moduleName: my_kmod
      kernelMappings:
        - regexp: '^.+$'
          containerImage: image-registry.openshift-image-registry.svc:5000/kmm-modules/my-kmod:${KERNEL_FULL_VERSION}
          build:
            dockerfileConfigMap:
              name: my-kmod-dockerfile
  selector:
    node-role.kubernetes.io/worker: ""
```

## Things to check

- **RBAC is namespace-wide.** Because `find` needs `list`, the ESO reader can read every Secret in `kmm-modules`. Keep the namespace limited to module and registry credentials. If other teams' Secrets live there, either move them or switch to a fixed-name token Secret, which allows `resourceNames`-scoped access.
- **Anchor the regex.** If the builder service account is recreated and an old `builder-dockercfg-*` secret lingers, a loose pattern matches both and the merge becomes unpredictable. The anchored `^builder-dockercfg-[a-z0-9]{5}$` keeps it to one match, and it also skips any `builder-token-*` secrets that older clusters create.
- **Rotation window.** The merged Secret can trail OpenShift's token rotation by up to `refreshInterval`. Keep it short (`1m`). If builds still hit expired tokens, check the builder token's lifetime and when it is rotated.
- **Read Vault directly if you can.** You can skip the intermediate `vault-registry-creds` Secret by giving the `external` entry a `sourceRef` that points at your Vault `ClusterSecretStore`. That gives you one sync chain instead of two.
- **Dotted key names.** If the `.dockerconfigjson` property lookup misbehaves, replace that entry with `dataFrom: - extract: {key: vault-registry-creds}` and reference it in the template as `index . ".dockerconfigjson"`.
- **Test the final output** before wiring it into KMM:

  ```bash
  oc get secret kmm-registry -n kmm-modules -o jsonpath='{.data.\.dockerconfigjson}' | base64 -d | jq .
  ```
