# Merging the KMM builder token and Vault registry credentials with External Secrets Operator

## Context

The Kernel Module Management (KMM) `Module` accepts a single `imageRepoSecret`. Two sets of registry credentials need to go into it:

- **Internal registry token.** The OpenShift `builder` service account's auto-generated pull secret (`builder-dockercfg-xxxxx`). It is type `kubernetes.io/dockercfg` (legacy format, no `"auths"` wrapper) and its JWT is rotated regularly.
- **External registry credentials.** A `kubernetes.io/dockerconfigjson` Secret already synced into the cluster from Vault.

External Secrets Operator (ESO) reads both through its Kubernetes provider and merges them into one `dockerconfigjson` Secret that KMM can use.

## 1. Find the builder secret name

The name has a random suffix:

```bash
oc get sa builder -n kmm-modules -o jsonpath='{.imagePullSecrets[*].name}'
# -> builder-dockercfg-7xk2p
```

## 2. Create a SecretStore that reads both in-cluster secrets

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
    resourceNames: ["builder-dockercfg-7xk2p", "vault-registry-creds"]
    verbs: ["get", "list", "watch"]
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

## 3. Create the ExternalSecret that merges them

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: kmm-registry
  namespace: kmm-modules
spec:
  refreshInterval: 1m                      # picks up the rotated token quickly
  secretStoreRef:
    name: in-cluster
    kind: SecretStore
  data:
    - secretKey: internal                  # legacy dockercfg (no "auths")
      remoteRef:
        key: builder-dockercfg-7xk2p
        property: .dockercfg
    - secretKey: external                  # dockerconfigjson synced from Vault
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
          {{- $internal := .internal | fromJson -}}
          {{- $external := (.external | fromJson).auths -}}
          {{- $merged := merge (dict) $external $internal -}}
          {{- dict "auths" $merged | toJson -}}
```

### What the template does

1. Parses the internal secret. Because it uses the legacy format, it is already a flat `{registry: creds}` map.
2. Unwraps `"auths"` from the Vault secret.
3. Merges the two maps. If both contain the same registry, the Vault entry wins because it comes first in `merge`.
4. Wraps the result back in `"auths"`.

### Example output

```json
{"auths":{
  "image-registry.openshift-image-registry.svc:5000":{"username":"<token>","password":"eyJhbGciOi...","auth":"...","email":"..."},
  "registry.example.com":{"username":"svc-kmm","password":"...","auth":"..."}
}}
```

## 4. Point the KMM Module at the merged Secret

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

- **Read Vault directly if you can.** You can skip the intermediate Secret by giving the `external` entry a `sourceRef` that points at your Vault `ClusterSecretStore`. That gives you one sync chain instead of two, and one less thing to lag behind.
- **Dotted key names.** If the `.dockercfg` / `.dockerconfigjson` property lookup misbehaves, replace the `data` entries with `dataFrom: - extract: {key: <secret>}`. Then reference the keys in the template as `index . ".dockercfg"` and `index . ".dockerconfigjson"`.
- **Rotation window.** The merged Secret can be up to `refreshInterval` behind the token rotation. A 1-minute refresh against a token rotated well before expiry is fine. If builds still hit expired tokens, lower the refresh interval or check how long the builder token is valid.
- **Test the output** before wiring it into KMM:

  ```bash
  oc get secret kmm-registry -n kmm-modules -o jsonpath='{.data.\.dockerconfigjson}' | base64 -d | jq .
  ```
