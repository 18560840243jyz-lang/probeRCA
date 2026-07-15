#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'usage: %s --action render|apply --stack-profile live|bounded\n' "$0" >&2
}

action=""
stack_profile=""
while (($#)); do
  case "$1" in
    --action)
      (($# >= 2)) || { printf 'missing value for action\n' >&2; exit 2; }
      action="$2"; shift 2 ;;
    --stack-profile)
      (($# >= 2)) || { printf 'missing value for stack-profile\n' >&2; exit 2; }
      stack_profile="$2"; shift 2 ;;
    --help|-h)
      usage; exit 0 ;;
    *)
      printf 'unknown argument: %s\n' "$1" >&2; usage; exit 2 ;;
  esac
done
[[ -n "$action" ]] || { printf 'action is required\n' >&2; usage; exit 2; }
[[ -n "$stack_profile" ]] || { printf 'stack-profile is required\n' >&2; usage; exit 2; }
case "$action" in render|apply) ;; *) printf 'unsupported action: %s\n' "$action" >&2; exit 2;; esac
case "$stack_profile" in live|bounded) ;; *) printf 'unsupported stack profile: %s\n' "$stack_profile" >&2; exit 2;; esac

context="${PROBERCA_P11_CONTEXT:?set PROBERCA_P11_CONTEXT}"
namespace="${PROBERCA_P11_SMOKE_NAMESPACE:?set PROBERCA_P11_SMOKE_NAMESPACE}"
run_id="${PROBERCA_P11_RUN_ID:?set PROBERCA_P11_RUN_ID}"
supply_mode="${PROBERCA_P11_IMAGE_SUPPLY_MODE:?set PROBERCA_P11_IMAGE_SUPPLY_MODE}"
render_dir="${PROBERCA_P11_RENDER_DIR:?set repository-external PROBERCA_P11_RENDER_DIR}"
[[ -n "$run_id" ]]
[[ "$(kubectl config current-context)" == "$context" ]]

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$render_dir"
render_dir="$(cd "$render_dir" && pwd)"
case "$render_dir/" in "$root/"*) printf 'render directory must be outside repository\n' >&2; exit 2;; esac
manifest_root="$root/deploy/kubernetes/test/p11-smoke"
profile_dir="$manifest_root/profiles/$stack_profile"
profile_manifest="$render_dir/profile-template.yaml"
source_manifest="$render_dir/source-template.yaml"
final_manifest="$render_dir/final-render.yaml"
summary="$render_dir/render-summary.json"
validator="$root/scripts/validate_p11_image_reference.py"

kubectl kustomize --load-restrictor=LoadRestrictionsNone "$profile_dir" > "$profile_manifest"
export P11_PROFILE_MANIFEST="$profile_manifest" P11_SOURCE_MANIFEST="$source_manifest"
export P11_NAMESPACE="$namespace" P11_RUN_ID="$run_id"
export P11_STACK_PROFILE="$stack_profile" P11_MAX_WINDOWS="${PROBERCA_P11_MAX_WINDOWS:-}"
python3 -B - <<'PY'
import os
from pathlib import Path
import yaml
source=Path(os.environ['P11_PROFILE_MANIFEST'])
target=Path(os.environ['P11_SOURCE_MANIFEST'])
namespace=os.environ['P11_NAMESPACE']; run_id=os.environ['P11_RUN_ID']
source_namespace='proberca-p11-smoke'
managed_by='proberca-p11-smoke'
run_placeholder='P11_SMOKE_RUN_ID'

def bind_name(value):
    if value==source_namespace: return namespace
    if value==source_namespace+'-reader': return namespace+'-reader'
    return value

def bind_metadata(metadata, *, bind_resource_name=True):
    if not isinstance(metadata,dict): return
    if bind_resource_name and 'name' in metadata:
        metadata['name']=bind_name(metadata['name'])
    if metadata.get('namespace')==source_namespace:
        metadata['namespace']=namespace
    labels=metadata.get('labels')
    if isinstance(labels,dict):
        if labels.get('app.kubernetes.io/managed-by') is not None:
            labels['app.kubernetes.io/managed-by']=managed_by
        if labels.get('proberca.io/smoke-run-id')==run_placeholder:
            labels['proberca.io/smoke-run-id']=run_id

def bind_config_data(document):
    if document.get('kind')!='ConfigMap': return
    data=document.get('data')
    if not isinstance(data,dict): return
    for key in ('config.yaml','prometheus.yml'):
        value=data.get(key)
        if isinstance(value,str):
            data[key]=value.replace(source_namespace,namespace)

def bind_document(document):
    bind_metadata(document.get('metadata',{}))
    template=document.get('spec',{}).get('template',{})
    bind_metadata(template.get('metadata',{}),bind_resource_name=False)
    if document.get('kind')=='ClusterRoleBinding':
        role_ref=document.get('roleRef',{})
        if role_ref.get('name')==source_namespace+'-reader':
            role_ref['name']=namespace+'-reader'
        for subject in document.get('subjects',[]):
            if subject.get('namespace')==source_namespace:
                subject['namespace']=namespace
    bind_config_data(document)
    return document

def remaining_dynamic_placeholder(value,path=()):
    if isinstance(value,dict):
        for key,item in value.items():
            yield from remaining_dynamic_placeholder(item,path+(str(key),))
    elif isinstance(value,list):
        for index,item in enumerate(value):
            yield from remaining_dynamic_placeholder(item,path+(str(index),))
    elif isinstance(value,str):
        if run_placeholder in value:
            yield path
        if source_namespace in value and path[-2:]!=(
            'labels','app.kubernetes.io/managed-by',
        ):
            yield path

documents=[bind_document(item) for item in yaml.safe_load_all(source.read_text()) if item]
remaining=[path for item in documents for path in remaining_dynamic_placeholder(item)]
if remaining:
    raise SystemExit('unbound smoke identity fields: '+', '.join('.'.join(path) for path in remaining))
max_windows=os.environ['P11_MAX_WINDOWS']
if max_windows:
    if os.environ['P11_STACK_PROFILE']!='bounded' or not max_windows.isdigit() or int(max_windows)<1:
        raise SystemExit('max windows override requires bounded profile and a positive integer')
    jobs=[item for item in documents if item.get('kind')=='Job' and item.get('metadata',{}).get('labels',{}).get('proberca.io/workload-role')=='bounded-runner']
    if len(jobs)!=1: raise SystemExit('max windows override requires exactly one bounded runner')
    replacements=0
    for container in jobs[0]['spec']['template']['spec'].get('containers',[]):
        args=container.get('args',[])
        for index,value in enumerate(args[:-1]):
            if value=='--max-windows':
                args[index+1]=max_windows; replacements+=1
    if replacements!=1: raise SystemExit('bounded runner must declare one max-windows argument')
override_path=os.environ.get('PROBERCA_P11_CONFIG_OVERRIDE')
if override_path:
    override=yaml.safe_load(Path(override_path).read_text())
    if not isinstance(override,dict): raise SystemExit('config override must be a mapping')
    matches=[]
    for item in documents:
        data=item.get('data',{}) if item.get('kind')=='ConfigMap' else {}
        if 'config.yaml' in data:
            config=yaml.safe_load(data['config.yaml'])
            if isinstance(config,dict) and 'live_liveness' in config: matches.append((data,config))
    if len(matches)!=1: raise SystemExit('config override requires exactly one live config')
    def merge(base, patch):
        for key,value in patch.items():
            if isinstance(value,dict) and isinstance(base.get(key),dict): merge(base[key],value)
            else: base[key]=value
    data,config=matches[0]; merge(config,override); data['config.yaml']=yaml.safe_dump(config,sort_keys=True)
target.write_text('\n---\n'.join(yaml.safe_dump(item,sort_keys=True).rstrip() for item in documents)+'\n')
PY
source_args=()
while IFS= read -r -d '' source_yaml; do
  source_args+=(--manifest "$source_yaml")
done < <(find "$manifest_root" -maxdepth 1 -type f -name '*.yaml' -print0 | sort -z)
[[ "${#source_args[@]}" -gt 0 ]]
python3 -B "$validator" "${source_args[@]}" --mode source-template

case "$supply_mode" in
  registry_digest)
    image="${PROBERCA_P11_IMAGE:?set digest PROBERCA_P11_IMAGE}"
    python3 -B "$validator" "$image"
    export P11_REGISTRY_IMAGE="$image" P11_FINAL_MANIFEST="$final_manifest"
    python3 -B - <<'PY'
import os
from pathlib import Path
import yaml
source=Path(os.environ['P11_SOURCE_MANIFEST']); target=Path(os.environ['P11_FINAL_MANIFEST']); image=os.environ['P11_REGISTRY_IMAGE']
def bind(value):
    if isinstance(value,str): return image if value=='PROBERCA_P11_IMAGE' else value
    if isinstance(value,list): return [bind(item) for item in value]
    if isinstance(value,dict): return {key:bind(item) for key,item in value.items()}
    return value
documents=[bind(item) for item in yaml.safe_load_all(source.read_text()) if item]
target.write_text('\n---\n'.join(yaml.safe_dump(item,sort_keys=True).rstrip() for item in documents)+'\n')
PY
    python3 -B "$validator" --manifest "$final_manifest" --mode final-registry ;;
  verified_local_import)
    identity="${PROBERCA_P11_IMAGE_IDENTITY_RECORD:?set PROBERCA_P11_IMAGE_IDENTITY_RECORD}"
    release_binding="${PROBERCA_P11_RELEASE_BINDING:?set PROBERCA_P11_RELEASE_BINDING}"
    evidence="${PROBERCA_P11_NODE_IMAGE_EVIDENCE:?set PROBERCA_P11_NODE_IMAGE_EVIDENCE}"
    python3 -B "$validator" --bind-local "$source_manifest" --output "$final_manifest" --identity-record "$identity"
    python3 -B "$validator" --manifest "$final_manifest" --mode final-local --identity-record "$identity" --release-binding "$release_binding" --node-image-evidence "$evidence" ;;
  *) printf 'unsupported image supply mode: %s\n' "$supply_mode" >&2; exit 2 ;;
esac

export P11_FINAL_MANIFEST="$final_manifest" P11_STACK_PROFILE="$stack_profile" P11_RENDER_SUMMARY="$summary"
python3 -B - <<'PY'
import json,os
from pathlib import Path
import yaml
documents=[item for item in yaml.safe_load_all(Path(os.environ['P11_FINAL_MANIFEST']).read_text()) if item]
role_key='proberca.io/workload-role'
def role(item): return item.get('metadata',{}).get('labels',{}).get(role_key)
live=[item for item in documents if role(item)=='live-runner']
bounded=[item for item in documents if role(item)=='bounded-runner']
shared=[item for item in documents if role(item)=='shared-observability']
profile=os.environ['P11_STACK_PROFILE']
if profile=='live':
    assert len(live)==1 and live[0].get('kind')=='Deployment' and not bounded
else:
    assert len(bounded)==1 and bounded[0].get('kind')=='Job' and not live
    assert bounded[0]['spec']['backoffLimit']==0
    assert bounded[0]['spec']['template']['spec']['restartPolicy']=='Never'
summary={'profile':profile,'resource_count':len(documents),'live_runner_count':len(live),'bounded_job_count':len(bounded),'shared_observability_count':len(shared),'workload_roles':sorted({role(item) for item in documents if role(item)}),'final_manifest':str(Path(os.environ['P11_FINAL_MANIFEST']))}
Path(os.environ['P11_RENDER_SUMMARY']).write_text(json.dumps(summary,sort_keys=True,separators=(',',':')))
print(json.dumps(summary,sort_keys=True,separators=(',',':')))
PY
printf 'rendered_manifest=%s\n' "$final_manifest"
[[ "$action" == render ]] && exit 0

foundation_manifest="$render_dir/apply-foundation.yaml"
workload_manifest="$render_dir/apply-workload.yaml"
warmup_file="$render_dir/bounded-warmup-sec"
export P11_FOUNDATION_MANIFEST="$foundation_manifest"
export P11_WORKLOAD_MANIFEST="$workload_manifest" P11_WARMUP_FILE="$warmup_file"
python3 -B - <<'PY'
import math, os
from pathlib import Path
import yaml

source = Path(os.environ["P11_FINAL_MANIFEST"])
foundation = Path(os.environ["P11_FOUNDATION_MANIFEST"])
workload = Path(os.environ["P11_WORKLOAD_MANIFEST"])
profile = os.environ["P11_STACK_PROFILE"]
target_role = "live-runner" if profile == "live" else "bounded-runner"
documents = [item for item in yaml.safe_load_all(source.read_text()) if item]
role_key = "proberca.io/workload-role"
selected = [
    item for item in documents
    if item.get("metadata", {}).get("labels", {}).get(role_key) == target_role
]
if len(selected) != 1:
    raise SystemExit("profile must select exactly one execution workload")
shared = [item for item in documents if item not in selected]

def write(path, values):
    path.write_text(
        "\n---\n".join(yaml.safe_dump(item, sort_keys=True).rstrip() for item in values) + "\n"
    )

write(foundation, shared)
write(workload, selected)
warmup = 0
if profile == "bounded":
    configs = []
    for item in documents:
        data = item.get("data", {}) if item.get("kind") == "ConfigMap" else {}
        if "config.yaml" in data:
            config = yaml.safe_load(data["config.yaml"])
            if isinstance(config, dict) and "live_liveness" in config:
                configs.append(config)
    if len(configs) != 1:
        raise SystemExit("bounded warmup requires exactly one live config")
    window_sec = configs[0]["live"]["window_sec"]
    if not isinstance(window_sec, (int, float)) or window_sec <= 0:
        raise SystemExit("bounded warmup window must be positive")
    warmup = math.ceil(float(window_sec)) + 1
Path(os.environ["P11_WARMUP_FILE"]).write_text(str(warmup))
PY
kubectl apply -f "$foundation_manifest" >/dev/null
kubectl -n "$namespace" rollout status deployment -l proberca.io/workload-role=shared-observability --timeout=180s
if [[ "$stack_profile" == bounded ]]; then
  python3 -B - "$(cat "$warmup_file")" <<'PY'
import sys, time
time.sleep(float(sys.argv[1]))
PY
fi
kubectl apply -f "$workload_manifest" >/dev/null
if [[ "$stack_profile" == live ]]; then
  kubectl -n "$namespace" rollout status deployment -l proberca.io/workload-role=live-runner --timeout=240s
else
  kubectl -n "$namespace" wait --for=condition=complete job -l proberca.io/workload-role=bounded-runner --timeout=540s
fi
printf 'smoke_stack_ready profile=%s\n' "$stack_profile"
