# Merging a KMM module's registry token and Vault registry credentials with External Secrets Operator

## Context

The Kernel Module Management (KMM) `Module` accepts a single `imageRepoSecret`. Two sets of registry credentials need to go into it:

- **Internal registry token.** Each KMM module runs under its own service account. Instead of relying on OpenShift's auto-generated `<sa>-dockercfg-xxxxx` secret (random suffix, legacy format), we create a **service account token Secret with a fixed, predictable name**.
- **External registry credentials.** A `kubernetes.io/dockerconfigjson` Secret already synced into the cluster from Vault.

External Secrets Operator (ESO) reads both through its Kubernetes provider and builds one `dockerconfigjson` Secret for the module. No name lookup and no legacy `dockercfg` parsing are needed.

Example names used below (replace with your own):

| Item | Name |
|---|---|
| Namespace | `kmm-modules` |
| Module service account | `kmm-my-kmod` |
| Token Secret (fixed name) | `kmm-my-kmod-token` |
| Vault-synced registry Secret | `vault-registry-creds` |
| Merged output Secret | `kmm-my-kmod-registry` |

## 1. Create the service account, push rights and a fixed-name token Secret

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kmm-my-kmod
  namespace: kmm-modules
---
apiVersion: v1
kind: Secret
metadata:
  name: kmm-my-kmod-token                 # fixed, predictable name
  namespace: kmm-modules
  annotations:
    kubernetes.io/service-account.name: kmm-my-kmod
type: kubernetes.io/service-account-token # Kubernetes fills in data.token
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: kmm-my-kmod-image-builder
  namespace: kmm-modules
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: system:image-builder              # push + pull on the internal registry
subjects:
  - kind: ServiceAccount
    name: kmm-my-kmod
    namespace: kmm-modules
```

Check that the token has been filled in:

```bash
oc get secret kmm-my-kmod-token -n kmm-modules -o jsonpath='{.data.token}' | base64 -d | cut -c1-20
```

## 2. Create a SecretStore that reads the in-cluster secrets

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
    resourceNames: ["kmm-my-kmod-token", "vault-registry-creds"]  # add one token per module
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

If you generate these resources per module, you can drop `resourceNames` and grant read access to Secrets across the namespace. This is simpler but broader.

## 3. Create the ExternalSecret that builds the merged Secret

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: kmm-my-kmod-registry
  namespace: kmm-modules
spec:
  refreshInterval: 15m
  secretStoreRef:
    name: in-cluster
    kind: SecretStore
  data:
    - secretKey: token                    # raw SA token (JWT)
      remoteRef:
        key: kmm-my-kmod-token
        property: token
    - secretKey: external                 # dockerconfigjson synced from Vault
      remoteRef:
        key: vault-registry-creds
        property: .dockerconfigjson
  target:
    name: kmm-my-kmod-registry
    creationPolicy: Owner
    template:
      type: kubernetes.io/dockerconfigjson
      engineVersion: v2
      data:
        .dockerconfigjson: |-
          {{- $reg := "image-registry.openshift-image-registry.svc:5000" -}}
          {{- $internal := dict $reg (dict "username" "serviceaccount" "password" .token "auth" (printf "serviceaccount:%s" .token | b64enc)) -}}
          {{- $external := (.external | fromJson).auths -}}
          {{- dict "auths" (merge (dict) $external $internal) | toJson -}}
```

### What the template does

1. Builds an `auths` entry for the internal registry from the raw token. The registry authenticates on the token alone, so `serviceaccount` is just a placeholder username.
2. Unwraps `"auths"` from the Vault secret.
3. Merges the two maps. If both contain the same registry, the Vault entry wins because it comes first in `merge`.
4. Wraps the result back in `"auths"`.

### Example output

```json
{"auths":{
  "image-registry.openshift-image-registry.svc:5000":{"username":"serviceaccount","password":"eyJhbGciOi...","auth":"..."},
  "registry.example.com":{"username":"svc-kmm","password":"...","auth":"..."}
}}
```

## 4. Point the KMM Module at the merged Secret and its service account

```yaml
apiVersion: kmm.sigs.x-k8s.io/v1beta1
kind: Module
metadata:
  name: my-kmod
  namespace: kmm-modules
spec:
  imageRepoSecret:
    name: kmm-my-kmod-registry
  moduleLoader:
    serviceAccountName: kmm-my-kmod
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

## Trade-off: the token does not expire

A `kubernetes.io/service-account-token` Secret holds a **long-lived JWT**. It stays valid until the Secret is deleted, unlike the rotated token in OpenShift's auto-generated secret. Two ways to handle this:

- **Accept it with controls.** The token is scoped to one service account with only `system:image-builder` in one namespace. Rotate it on a schedule by deleting and recreating the Secret; ESO picks up the new token on its next refresh.
- **Have the tenant operator mint short-lived tokens (recommended for the platform).** Have the platform's custom tenant operator call the `TokenRequest` API for each module's service account (for example, 1h expiry, renewed at about 45 minutes) and write the result into the same fixed-name Secret, as a plain Secret with a `token` key. The ExternalSecret above works unchanged, and you get both a predictable name and rotation. If you do this, lower `refreshInterval` (for example, `1m`) so the merged Secret keeps up with each rotation.

## Things to check

- **Read Vault directly if you can.** You can skip the intermediate `vault-registry-creds` Secret by giving the `external` entry a `sourceRef` that points at your Vault `ClusterSecretStore`. That gives you one sync chain instead of two.
- **Dotted key names.** If the `.dockerconfigjson` property lookup misbehaves, replace that entry with `dataFrom: - extract: {key: vault-registry-creds}` and reference it in the template as `index . ".dockerconfigjson"`.
- **Test the output** before wiring it into KMM:

  ```bash
  oc get secret kmm-my-kmod-registry -n kmm-modules -o jsonpath='{.data.\.dockerconfigjson}' | base64 -d | jq .
  ```
