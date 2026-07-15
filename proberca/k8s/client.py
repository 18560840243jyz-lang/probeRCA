"""Official Kubernetes Python-client discovery adapter."""
from __future__ import annotations

import json

from .contracts import canonical_hash
from .inventory import KubernetesInventory
from .supervisor import KubernetesWatchSupervisor
from .watch import KubernetesListWatcher, WatchExpiredError


class KubernetesDiscoveryError(RuntimeError):
    """Kubernetes configuration or discovery failed."""


def _decode_raw_collection(kind, response):
    """Decode a list response without constructing strict client model objects."""
    payload = response.data
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    value = json.loads(payload)
    if not isinstance(value, dict) or not isinstance(value.get("items"), list):
        raise KubernetesDiscoveryError(
            f"invalid Kubernetes collection payload for kind={kind}")
    objects = []
    for item in value["items"]:
        if not isinstance(item, dict):
            raise KubernetesDiscoveryError(
                f"invalid Kubernetes object payload for kind={kind}")
        item = dict(item)
        item["kind"] = kind
        item.setdefault("apiVersion", "v1")
        objects.append(item)
    metadata = value.get("metadata") or {}
    return objects, str(metadata.get("resourceVersion") or "")


REQUIRED_RESOURCE_KINDS = (
    "Pod", "Service", "EndpointSlice", "Node", "Deployment", "ReplicaSet",
    "StatefulSet", "DaemonSet", "Job", "PersistentVolumeClaim", "PersistentVolume",
)


class KubernetesDiscoveryClient:
    def __init__(self, config, *, list_adapters=None, watch_adapters=None):
        config.validate()
        self.config = config
        if list_adapters is None:
            self.list_adapters, self.watch_adapters = self._official_watch_bundle(config)
        else:
            self.list_adapters = list_adapters
            self.watch_adapters = watch_adapters or {}

    def _required_kinds(self):
        required = list(REQUIRED_RESOURCE_KINDS)
        if not self.config.include_jobs:
            required.remove("Job")
        if not self.config.include_persistent_volumes:
            required = [item for item in required
                        if item not in {"PersistentVolumeClaim", "PersistentVolume"}]
        if self.config.include_volume_attachments:
            required.append("VolumeAttachment")
        return required

    @staticmethod
    def _official_adapters(config):
        from urllib.parse import urlparse
        try:
            from kubernetes import client, config as kube_config
        except ImportError as error:
            raise KubernetesDiscoveryError("official Kubernetes client is unavailable") from error
        if config.in_cluster:
            kube_config.load_incluster_config()
        else:
            kube_config.load_kube_config(
                config_file=config.kubeconfig_path, context=config.context)
        configuration = client.Configuration.get_default_copy()
        api_host = urlparse(configuration.host).hostname
        no_proxy = [item for item in (configuration.no_proxy or "").split(",") if item]
        if api_host and api_host not in no_proxy:
            no_proxy.append(api_host)
        configuration.no_proxy = ",".join(no_proxy)
        api_client = client.ApiClient(configuration)
        core, apps = client.CoreV1Api(api_client), client.AppsV1Api(api_client)
        batch, discovery = client.BatchV1Api(api_client), client.DiscoveryV1Api(api_client)
        storage = client.StorageV1Api(api_client)

        def convert(kind, response):
            objects = []
            for item in response.items:
                raw = api_client.sanitize_for_serialization(item)
                raw["kind"] = kind
                raw.setdefault("apiVersion", getattr(item, "api_version", None) or "v1")
                objects.append(raw)
            metadata = api_client.sanitize_for_serialization(response.metadata)
            return objects, str(metadata.get("resourceVersion") or metadata.get("resource_version") or "")

        def namespaced(kind, method):
            def load(namespaces):
                all_objects, revisions = [], {}
                for namespace in namespaces:
                    response = method(namespace=namespace, watch=False)
                    objects, rv = convert(kind, response)
                    all_objects.extend(objects)
                    revisions[namespace] = rv
                return all_objects, canonical_hash(revisions)
            return load

        def cluster(kind, method):
            def load(_namespaces):
                return convert(kind, method(watch=False))
            return load

        return {
            "Pod": namespaced("Pod", core.list_namespaced_pod),
            "Service": namespaced("Service", core.list_namespaced_service),
            "EndpointSlice": namespaced("EndpointSlice", discovery.list_namespaced_endpoint_slice),
            "Node": cluster("Node", core.list_node),
            "Deployment": namespaced("Deployment", apps.list_namespaced_deployment),
            "ReplicaSet": namespaced("ReplicaSet", apps.list_namespaced_replica_set),
            "StatefulSet": namespaced("StatefulSet", apps.list_namespaced_stateful_set),
            "DaemonSet": namespaced("DaemonSet", apps.list_namespaced_daemon_set),
            "Job": namespaced("Job", batch.list_namespaced_job),
            "PersistentVolumeClaim": namespaced(
                "PersistentVolumeClaim", core.list_namespaced_persistent_volume_claim),
            "PersistentVolume": cluster("PersistentVolume", core.list_persistent_volume),
            "VolumeAttachment": cluster("VolumeAttachment", storage.list_volume_attachment),
        }

    @staticmethod
    def _official_watch_bundle(config):
        from urllib.parse import urlparse
        try:
            from kubernetes import client, config as kube_config, watch
            from kubernetes.client.exceptions import ApiException
        except ImportError as error:
            raise KubernetesDiscoveryError("official Kubernetes client is unavailable") from error
        if config.in_cluster:
            kube_config.load_incluster_config()
        else:
            kube_config.load_kube_config(
                config_file=config.kubeconfig_path, context=config.context)
        configuration = client.Configuration.get_default_copy()
        api_host = urlparse(configuration.host).hostname
        no_proxy = [item for item in (configuration.no_proxy or "").split(",") if item]
        if api_host and api_host not in no_proxy:
            no_proxy.append(api_host)
        configuration.no_proxy = ",".join(no_proxy)
        api_client = client.ApiClient(configuration)
        core, apps = client.CoreV1Api(api_client), client.AppsV1Api(api_client)
        batch, discovery = client.BatchV1Api(api_client), client.DiscoveryV1Api(api_client)
        storage = client.StorageV1Api(api_client)
        methods = {
            "Pod": core.list_pod_for_all_namespaces,
            "Service": core.list_service_for_all_namespaces,
            "EndpointSlice": discovery.list_endpoint_slice_for_all_namespaces,
            "Node": core.list_node,
            "Deployment": apps.list_deployment_for_all_namespaces,
            "ReplicaSet": apps.list_replica_set_for_all_namespaces,
            "StatefulSet": apps.list_stateful_set_for_all_namespaces,
            "DaemonSet": apps.list_daemon_set_for_all_namespaces,
            "Job": batch.list_job_for_all_namespaces,
            "PersistentVolumeClaim": core.list_persistent_volume_claim_for_all_namespaces,
            "PersistentVolume": core.list_persistent_volume,
            "VolumeAttachment": storage.list_volume_attachment,
        }
        cluster_scoped = {"Node", "PersistentVolume", "VolumeAttachment"}

        def raw(kind, item):
            value = api_client.sanitize_for_serialization(item)
            value["kind"] = kind
            value.setdefault("apiVersion", getattr(item, "api_version", None) or "v1")
            return value

        def allowed(kind, value):
            return kind in cluster_scoped or (value.get("metadata") or {}).get("namespace") in config.namespaces

        list_adapters, watch_adapters = {}, {}
        for kind, method in methods.items():
            def list_call(_namespaces, kind=kind, method=method):
                if kind == "EndpointSlice":
                    response = method(watch=False, _preload_content=False)
                    values, rv = _decode_raw_collection(kind, response)
                else:
                    response = method(watch=False)
                    values = [raw(kind, item) for item in response.items]
                    metadata = api_client.sanitize_for_serialization(response.metadata)
                    rv = metadata.get("resourceVersion") or metadata.get("resource_version") or ""
                return [value for value in values if allowed(kind, value)], str(rv)

            class Stream:
                def __init__(self, resource_version, *, _kind=kind, _method=method):
                    self._kind, self._watch = _kind, watch.Watch()
                    self._resource_version, self._method = resource_version, _method

                def __iter__(self):
                    try:
                        for event in self._watch.stream(
                                self._method, resource_version=self._resource_version,
                                timeout_seconds=int(config.watch_timeout_sec),
                                allow_watch_bookmarks=config.allow_watch_bookmarks,
                                deserialize=False):
                            if event.get("type") == "ERROR":
                                status = api_client.sanitize_for_serialization(
                                    event.get("object")) or {}
                                if int(status.get("code") or 0) == 410:
                                    raise WatchExpiredError(
                                        "Kubernetes resourceVersion expired")
                                raise KubernetesDiscoveryError(
                                    f"Kubernetes watch ERROR kind={self._kind} "
                                    f"reason={status.get('reason') or 'unknown'}")
                            value = raw(self._kind, event["object"])
                            if allowed(self._kind, value):
                                yield {"type": event["type"], "object": value}
                    except ApiException as error:
                        if error.status == 410:
                            raise WatchExpiredError("Kubernetes resourceVersion expired") from error
                        raise

                def stop(self):
                    self._watch.stop()

            list_adapters[kind] = list_call
            watch_adapters[kind] = lambda rv, stream=Stream: stream(rv)
        return list_adapters, watch_adapters

    def create_supervisor(self):
        required = self._required_kinds()
        missing = sorted(set(required) - set(self.list_adapters))
        missing_watch = sorted(set(required) - set(self.watch_adapters))
        if missing or missing_watch:
            raise KubernetesDiscoveryError(
                f"missing Kubernetes adapters: list={missing} watch={missing_watch}")
        inventory = KubernetesInventory(
            self.config.cluster_id, required_kinds=tuple(required),
            stale_after_sec=self.config.inventory_stale_after_sec,
            namespace_scope=self.config.namespaces,
            endpoint_ready_policy=self.config.endpoint_ready_policy,
            include_terminating_endpoints=self.config.include_terminating_endpoints)
        watchers = [KubernetesListWatcher(
            kind, inventory,
            lambda kind=kind: self.list_adapters[kind](self.config.namespaces),
            self.watch_adapters[kind],
            reconnect_initial_sec=self.config.reconnect_initial_sec,
            reconnect_max_sec=self.config.reconnect_max_sec)
            for kind in required]
        return KubernetesWatchSupervisor(inventory, watchers)

    def discover_once(self, observed_at_ns: int):
        required = self._required_kinds()
        missing = sorted(set(required) - set(self.list_adapters))
        if missing:
            raise KubernetesDiscoveryError(f"missing Kubernetes list adapters: {missing}")
        inventory = KubernetesInventory(
            self.config.cluster_id, required_kinds=tuple(required),
            stale_after_sec=self.config.inventory_stale_after_sec,
            namespace_scope=self.config.namespaces,
            endpoint_ready_policy=self.config.endpoint_ready_policy,
            include_terminating_endpoints=self.config.include_terminating_endpoints)
        for kind in required:
            try:
                objects, rv = self.list_adapters[kind](self.config.namespaces)
                inventory.replace_kind(kind, list(objects), str(rv), observed_at_ns)
            except Exception as error:
                raise KubernetesDiscoveryError(f"initial list failed for kind={kind}") from error
        return inventory
