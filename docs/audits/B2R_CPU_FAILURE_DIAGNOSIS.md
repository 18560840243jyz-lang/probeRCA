# B2R CPU Failure Diagnosis

Labels are used only for post-hoc diagnosis, not for inference or ranking repair.

## B2 CPU Summary

- `auxiliary_metric_hit_at_1_mean`: 0.0
- `auxiliary_metric_mrr_mean`: 0.0
- `fault_type`: cpu
- `metric_hit_at_3_mean`: 0.0
- `path_fidelity_mean`: 1.0
- `repeats`: 5
- `repeats_completed`: 5
- `root_type_accuracy_mean`: 0.0
- `service_hit_at_1_mean`: 0.0

## CPU Repeat 01

- top1_service: redis-cart
- top1_metric: redis-cart.memory.usage
- predicted_root_type: memory

### Top 10 Metric Candidates

- rank 1: `redis-cart.memory.usage` family=memory final=0.97039672693337 metric_score=9.090993726001603 service_score=8.155294772725911 evidence=2.7193165955379177 cf_delta=18.535723952182025 components={'counterfactual_norm': 1.0, 'evidence_norm': 0.8283174892670443, 'metric_score_norm': 1.0, 'service_score_norm': 0.9378248900333271, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 2: `emailservice.memory.usage` family=memory final=0.9206929537527481 metric_score=8.521874537049708 service_score=8.269992632143891 evidence=2.23606797749979 cf_delta=18.141488584677745 components={'counterfactual_norm': 0.9787310509952933, 'evidence_norm': 0.68111753371868, 'metric_score_norm': 0.9373974720360736, 'service_score_norm': 0.9510146655587274, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 3: `shippingservice.memory.usage` family=memory final=0.9193593372553408 metric_score=8.52187410149932 service_score=8.15529607370144 evidence=2.23606797749979 cf_delta=18.302667212575443 components={'counterfactual_norm': 0.9874266179077863, 'evidence_norm': 0.68111753371868, 'metric_score_norm': 0.9373974241259769, 'service_score_norm': 0.9378250396400877, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 4: `cartservice.memory.usage` family=memory final=0.9183821267780504 metric_score=8.521875247061205 service_score=8.244309850813503 evidence=2.23606797749979 cf_delta=17.92892246191309 components={'counterfactual_norm': 0.9672631351311475, 'evidence_norm': 0.68111753371868, 'metric_score_norm': 0.9373975501366112, 'service_score_norm': 0.9480612528068704, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 5: `recommendationservice.memory.usage` family=memory final=0.916808326803827 metric_score=8.521875555354873 service_score=8.117514420560015 evidence=2.23606797749979 cf_delta=18.094801191091847 components={'counterfactual_norm': 0.9762122719227121, 'evidence_norm': 0.68111753371868, 'metric_score_norm': 0.9373975840485989, 'service_score_norm': 0.9334803070841128, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 6: `emailservice.cpu.usage` family=CPU final=0.9157531657873283 metric_score=8.40365090720981 service_score=8.269992632143891 evidence=2.23606797749979 cf_delta=18.414912144343134 components={'counterfactual_norm': 0.9934822180050502, 'evidence_norm': 0.68111753371868, 'metric_score_norm': 0.9243929938235587, 'service_score_norm': 0.9510146655587274, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 7: `adservice.cpu.throttled_usec` family=CPU final=0.8232508326776133 metric_score=9.06226797336229 service_score=8.695967508855624 evidence=2.3689225843685664 cf_delta=0.34972624730767166 components={'counterfactual_norm': 0.01886768751033875, 'evidence_norm': 0.721585714062108, 'metric_score_norm': 0.9968401966270032, 'service_score_norm': 1.0, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 8: `adservice.cpu.throttle_ratio` family=CPU final=0.8176782494690724 metric_score=8.996914373285424 service_score=8.695967508855624 evidence=2.315780741621056 cf_delta=0.34972624730767166 components={'counterfactual_norm': 0.01886768751033875, 'evidence_norm': 0.7053984419247368, 'metric_score_norm': 0.9896513675455415, 'service_score_norm': 1.0, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 9: `currencyservice.cpu.throttled_usec` family=CPU final=0.807026766665645 metric_score=8.871432096412079 service_score=8.669142231135192 evidence=2.23606797749979 cf_delta=-0.3478941894113632 components={'counterfactual_norm': 0.018768848214876933, 'evidence_norm': 0.68111753371868, 'metric_score_norm': 0.9758484455927469, 'service_score_norm': 0.9969152049276732, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 10: `currencyservice.cpu.throttle_ratio` family=CPU final=0.8048865727620008 metric_score=8.871433383068831 service_score=8.669142231135192 evidence=2.23606797749979 cf_delta=0.08341761434417094 components={'counterfactual_norm': 0.004500369910523566, 'evidence_norm': 0.68111753371868, 'metric_score_norm': 0.9758485871236721, 'service_score_norm': 0.9969152049276732, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}

### Top 10 Services

- rank 1: `redis-cart` final=0.97039672693337 top_metric=redis-cart.memory.usage a8r_service=8.155294772725911
- rank 2: `emailservice` final=0.9206929537527481 top_metric=emailservice.memory.usage a8r_service=8.269992632143891
- rank 3: `shippingservice` final=0.9193593372553408 top_metric=shippingservice.memory.usage a8r_service=8.15529607370144
- rank 4: `cartservice` final=0.9183821267780504 top_metric=cartservice.memory.usage a8r_service=8.244309850813503
- rank 5: `recommendationservice` final=0.916808326803827 top_metric=recommendationservice.memory.usage a8r_service=8.117514420560015

### Memory vs CPU Candidate Components

#### memory
- rank 1: `redis-cart.memory.usage` final=0.97039672693337 components={'counterfactual_norm': 1.0, 'evidence_norm': 0.8283174892670443, 'metric_score_norm': 1.0, 'service_score_norm': 0.9378248900333271, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 2: `emailservice.memory.usage` final=0.9206929537527481 components={'counterfactual_norm': 0.9787310509952933, 'evidence_norm': 0.68111753371868, 'metric_score_norm': 0.9373974720360736, 'service_score_norm': 0.9510146655587274, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 3: `shippingservice.memory.usage` final=0.9193593372553408 components={'counterfactual_norm': 0.9874266179077863, 'evidence_norm': 0.68111753371868, 'metric_score_norm': 0.9373974241259769, 'service_score_norm': 0.9378250396400877, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 4: `cartservice.memory.usage` final=0.9183821267780504 components={'counterfactual_norm': 0.9672631351311475, 'evidence_norm': 0.68111753371868, 'metric_score_norm': 0.9373975501366112, 'service_score_norm': 0.9480612528068704, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 5: `recommendationservice.memory.usage` final=0.916808326803827 components={'counterfactual_norm': 0.9762122719227121, 'evidence_norm': 0.68111753371868, 'metric_score_norm': 0.9373975840485989, 'service_score_norm': 0.9334803070841128, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
#### cpu
- rank 6: `emailservice.cpu.usage` final=0.9157531657873283 components={'counterfactual_norm': 0.9934822180050502, 'evidence_norm': 0.68111753371868, 'metric_score_norm': 0.9243929938235587, 'service_score_norm': 0.9510146655587274, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 7: `adservice.cpu.throttled_usec` final=0.8232508326776133 components={'counterfactual_norm': 0.01886768751033875, 'evidence_norm': 0.721585714062108, 'metric_score_norm': 0.9968401966270032, 'service_score_norm': 1.0, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 8: `adservice.cpu.throttle_ratio` final=0.8176782494690724 components={'counterfactual_norm': 0.01886768751033875, 'evidence_norm': 0.7053984419247368, 'metric_score_norm': 0.9896513675455415, 'service_score_norm': 1.0, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 9: `currencyservice.cpu.throttled_usec` final=0.807026766665645 components={'counterfactual_norm': 0.018768848214876933, 'evidence_norm': 0.68111753371868, 'metric_score_norm': 0.9758484455927469, 'service_score_norm': 0.9969152049276732, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 10: `currencyservice.cpu.throttle_ratio` final=0.8048865727620008 components={'counterfactual_norm': 0.004500369910523566, 'evidence_norm': 0.68111753371868, 'metric_score_norm': 0.9758485871236721, 'service_score_norm': 0.9969152049276732, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}

### Post-hoc Labels For Diagnosis Only

- incident_id: ob-cpu-paymentservice-repeat-01
- root_service: paymentservice
- root_metric: cpu.throttled_usec
- root_type: CPU throttling

## CPU Repeat 02

- top1_service: shippingservice
- top1_metric: shippingservice.memory.usage
- predicted_root_type: memory

### Top 10 Metric Candidates

- rank 1: `shippingservice.memory.usage` family=memory final=0.9356409336766627 metric_score=9.480179942391569 service_score=8.118042682323509 evidence=3.1156852852568626 cf_delta=18.792585525331788 components={'counterfactual_norm': 0.9925440062683805, 'evidence_norm': 0.9126366193478003, 'metric_score_norm': 0.9250573331153206, 'service_score_norm': 0.9335706879409958, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 2: `adservice.memory.usage` family=memory final=0.8767295351214676 metric_score=8.75997062287777 service_score=8.105373398643726 evidence=2.4991347983824412 cf_delta=18.551739325441986 components={'counterfactual_norm': 0.9798235399008917, 'evidence_norm': 0.7320386126553743, 'metric_score_norm': 0.8547807227088978, 'service_score_norm': 0.9321137269045124, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 3: `emailservice.memory.usage` family=memory final=0.8584983925323538 metric_score=8.452675097311241 service_score=8.233198737669227 evidence=2.23606797749979 cf_delta=18.933755487563076 components={'counterfactual_norm': 1.0, 'evidence_norm': 0.6549819165863028, 'metric_score_norm': 0.8247954290661348, 'service_score_norm': 0.946813574436747, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 4: `emailservice.cpu.usage` family=CPU final=0.851779743076433 metric_score=8.397869180075391 service_score=8.233198737669227 evidence=2.23606797749979 cf_delta=18.45696188197735 components={'counterfactual_norm': 0.9748178006260345, 'evidence_norm': 0.6549819165863028, 'metric_score_norm': 0.8194475753391786, 'service_score_norm': 0.946813574436747, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 5: `recommendationservice.memory.usage` family=memory final=0.8502236468173484 metric_score=8.4526734149791 service_score=8.054973583849351 evidence=2.23606797749979 cf_delta=18.406703425223895 components={'counterfactual_norm': 0.9721633638564001, 'evidence_norm': 0.6549819165863028, 'metric_score_norm': 0.8247952649074698, 'service_score_norm': 0.9263177744057488, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 6: `currencyservice.cpu.throttled_usec` family=CPU final=0.8491578461253813 metric_score=10.248207979136943 service_score=8.61258750796759 evidence=3.4139384933769503 cf_delta=-0.13496346580427598 components={'counterfactual_norm': 0.007128193130672295, 'evidence_norm': 1.0, 'metric_score_norm': 1.0, 'service_score_norm': 0.9904430857789018, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 7: `checkoutservice.cpu.usage` family=CPU final=0.8372566929502904 metric_score=8.13327190308879 service_score=8.105376906908342 evidence=2.23606797749979 cf_delta=18.787321080916854 components={'counterfactual_norm': 0.992265960826292, 'evidence_norm': 0.6549819165863028, 'metric_score_norm': 0.7936286929038043, 'service_score_norm': 0.9321141303531194, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 8: `shippingservice.cpu.usage` family=CPU final=0.8368696316875125 metric_score=8.159691631719452 service_score=8.118042682323509 evidence=2.23606797749979 cf_delta=18.522719999091805 components={'counterfactual_norm': 0.978290863176021, 'evidence_norm': 0.6549819165863028, 'metric_score_norm': 0.7962066781168725, 'service_score_norm': 0.9335706879409958, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 9: `adservice.cpu.usage` family=CPU final=0.8342238020174487 metric_score=8.13326785426215 service_score=8.105373398643726 evidence=2.23606797749979 cf_delta=18.404531924730918 components={'counterfactual_norm': 0.972048674486169, 'evidence_norm': 0.6549819165863028, 'metric_score_norm': 0.7936282978272556, 'service_score_norm': 0.9321137269045124, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 10: `currencyservice.cpu.throttle_ratio` family=CPU final=0.8043570295723066 metric_score=9.674349867288498 service_score=8.61258750796759 evidence=2.942790287026086 cf_delta=-0.10942532448711972 components={'counterfactual_norm': 0.005779377712942232, 'evidence_norm': 0.8619927666345211, 'metric_score_norm': 0.9440040529020595, 'service_score_norm': 0.9904430857789018, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}

### Top 10 Services

- rank 1: `shippingservice` final=0.9356409336766627 top_metric=shippingservice.memory.usage a8r_service=8.118042682323509
- rank 2: `adservice` final=0.8767295351214676 top_metric=adservice.memory.usage a8r_service=8.105373398643726
- rank 3: `emailservice` final=0.8584983925323538 top_metric=emailservice.memory.usage a8r_service=8.233198737669227
- rank 4: `recommendationservice` final=0.8502236468173484 top_metric=recommendationservice.memory.usage a8r_service=8.054973583849351
- rank 5: `currencyservice` final=0.8491578461253813 top_metric=currencyservice.cpu.throttled_usec a8r_service=8.61258750796759

### Memory vs CPU Candidate Components

#### memory
- rank 1: `shippingservice.memory.usage` final=0.9356409336766627 components={'counterfactual_norm': 0.9925440062683805, 'evidence_norm': 0.9126366193478003, 'metric_score_norm': 0.9250573331153206, 'service_score_norm': 0.9335706879409958, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 2: `adservice.memory.usage` final=0.8767295351214676 components={'counterfactual_norm': 0.9798235399008917, 'evidence_norm': 0.7320386126553743, 'metric_score_norm': 0.8547807227088978, 'service_score_norm': 0.9321137269045124, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 3: `emailservice.memory.usage` final=0.8584983925323538 components={'counterfactual_norm': 1.0, 'evidence_norm': 0.6549819165863028, 'metric_score_norm': 0.8247954290661348, 'service_score_norm': 0.946813574436747, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 5: `recommendationservice.memory.usage` final=0.8502236468173484 components={'counterfactual_norm': 0.9721633638564001, 'evidence_norm': 0.6549819165863028, 'metric_score_norm': 0.8247952649074698, 'service_score_norm': 0.9263177744057488, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 19: `checkoutservice.memory.usage` final=0.5781343072921731 components={'counterfactual_norm': 0.0, 'evidence_norm': 0.2492027731852696, 'metric_score_norm': 0.6668930980054949, 'service_score_norm': 0.9321141303531194, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
#### cpu
- rank 4: `emailservice.cpu.usage` final=0.851779743076433 components={'counterfactual_norm': 0.9748178006260345, 'evidence_norm': 0.6549819165863028, 'metric_score_norm': 0.8194475753391786, 'service_score_norm': 0.946813574436747, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 6: `currencyservice.cpu.throttled_usec` final=0.8491578461253813 components={'counterfactual_norm': 0.007128193130672295, 'evidence_norm': 1.0, 'metric_score_norm': 1.0, 'service_score_norm': 0.9904430857789018, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 7: `checkoutservice.cpu.usage` final=0.8372566929502904 components={'counterfactual_norm': 0.992265960826292, 'evidence_norm': 0.6549819165863028, 'metric_score_norm': 0.7936286929038043, 'service_score_norm': 0.9321141303531194, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 8: `shippingservice.cpu.usage` final=0.8368696316875125 components={'counterfactual_norm': 0.978290863176021, 'evidence_norm': 0.6549819165863028, 'metric_score_norm': 0.7962066781168725, 'service_score_norm': 0.9335706879409958, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 9: `adservice.cpu.usage` final=0.8342238020174487 components={'counterfactual_norm': 0.972048674486169, 'evidence_norm': 0.6549819165863028, 'metric_score_norm': 0.7936282978272556, 'service_score_norm': 0.9321137269045124, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}

### Post-hoc Labels For Diagnosis Only

- incident_id: ob-cpu-paymentservice-repeat-02
- root_service: paymentservice
- root_metric: cpu.throttled_usec
- root_type: CPU throttling

## CPU Repeat 03

- top1_service: recommendationservice
- top1_metric: recommendationservice.memory.usage
- predicted_root_type: memory

### Top 10 Metric Candidates

- rank 1: `recommendationservice.memory.usage` family=memory final=0.9757038859274725 metric_score=8.917914079789925 service_score=8.048558385613534 evidence=2.6875667206918945 cf_delta=18.193577140316393 components={'counterfactual_norm': 0.9907344197457393, 'evidence_norm': 1.0, 'metric_score_norm': 0.9854139464874652, 'service_score_norm': 0.9255802619875287, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 2: `adservice.memory.usage` family=memory final=0.9755804538339509 metric_score=8.895314666920186 service_score=8.07399028618625 evidence=2.668088776637044 cf_delta=18.36372773339758 components={'counterfactual_norm': 1.0, 'evidence_norm': 0.9927525728366527, 'metric_score_norm': 0.9829167507951745, 'service_score_norm': 0.9285049180646978, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 3: `redis-cart.memory.usage` family=memory final=0.9506580056053604 metric_score=8.63546014596996 service_score=8.061221297289523 evidence=2.4440924200058527 cf_delta=18.30229343144788 components={'counterfactual_norm': 0.996654584360997, 'evidence_norm': 0.9094071604580067, 'metric_score_norm': 0.9542032796054852, 'service_score_norm': 0.9270364906119661, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 4: `adservice.cpu.usage` family=CPU final=0.9210544872515181 metric_score=8.231552815239834 service_score=8.07399028618625 evidence=2.327807405340112 cf_delta=18.177010380747106 components={'counterfactual_norm': 0.9898322739608637, 'evidence_norm': 0.8661393919704569, 'metric_score_norm': 0.9095722242680058, 'service_score_norm': 0.9285049180646978, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 5: `redis-cart.cpu.usage` family=CPU final=0.9102607276971638 metric_score=8.101921347898674 service_score=8.061221297289523 evidence=2.23606797749979 cf_delta=18.273925484105803 components={'counterfactual_norm': 0.9951098028354854, 'evidence_norm': 0.8320046383533617, 'metric_score_norm': 0.8952481732983847, 'service_score_norm': 0.9270364906119661, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 6: `recommendationservice.cpu.usage` family=CPU final=0.9072717499386397 metric_score=8.075497493736611 service_score=8.048558385613534 evidence=2.23606797749979 cf_delta=18.140256197232475 components={'counterfactual_norm': 0.9878308184803521, 'evidence_norm': 0.8320046383533617, 'metric_score_norm': 0.8923283835158998, 'service_score_norm': 0.9255802619875287, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 7: `currencyservice.cpu.throttled_usec` family=CPU final=0.8412540941536077 metric_score=9.049916648306098 service_score=8.612568370557879 evidence=2.430138323274363 cf_delta=-0.3359779590882681 components={'counterfactual_norm': 0.018295738423372215, 'evidence_norm': 0.9042150673188653, 'metric_score_norm': 1.0, 'service_score_norm': 0.9904411332910764, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 8: `currencyservice.cpu.usage` family=CPU final=0.8326194211094571 metric_score=8.955365490480878 service_score=8.612568370557879 evidence=2.3525101849645336 cf_delta=-0.33597795908804073 components={'counterfactual_norm': 0.018295738423359836, 'evidence_norm': 0.8753308957326636, 'metric_score_norm': 0.9895522620263119, 'service_score_norm': 0.9904411332910764, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 9: `emailservice.cpu.usage` family=CPU final=0.7902921828483117 metric_score=7.757077960352735 service_score=5.3600090439123855 evidence=2.23606797749979 cf_delta=13.758480426712595 components={'counterfactual_norm': 0.7492204538455688, 'evidence_norm': 0.8320046383533617, 'metric_score_norm': 0.857143580632276, 'service_score_norm': 0.616398407941942, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 10: `productcatalogservice.cpu.usage` family=CPU final=0.7773281296061549 metric_score=7.641324983815541 service_score=5.280025704274669 evidence=2.23606797749979 cf_delta=13.257803402774243 components={'counterfactual_norm': 0.721955999089589, 'evidence_norm': 0.8320046383533617, 'metric_score_norm': 0.8443530786823095, 'service_score_norm': 0.6072003631605508, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}

### Top 10 Services

- rank 1: `recommendationservice` final=0.9757038859274725 top_metric=recommendationservice.memory.usage a8r_service=8.048558385613534
- rank 2: `adservice` final=0.9755804538339509 top_metric=adservice.memory.usage a8r_service=8.07399028618625
- rank 3: `redis-cart` final=0.9506580056053604 top_metric=redis-cart.memory.usage a8r_service=8.061221297289523
- rank 4: `currencyservice` final=0.8412540941536077 top_metric=currencyservice.cpu.throttled_usec a8r_service=8.612568370557879
- rank 5: `emailservice` final=0.7902921828483117 top_metric=emailservice.cpu.usage a8r_service=5.3600090439123855

### Memory vs CPU Candidate Components

#### memory
- rank 1: `recommendationservice.memory.usage` final=0.9757038859274725 components={'counterfactual_norm': 0.9907344197457393, 'evidence_norm': 1.0, 'metric_score_norm': 0.9854139464874652, 'service_score_norm': 0.9255802619875287, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 2: `adservice.memory.usage` final=0.9755804538339509 components={'counterfactual_norm': 1.0, 'evidence_norm': 0.9927525728366527, 'metric_score_norm': 0.9829167507951745, 'service_score_norm': 0.9285049180646978, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 3: `redis-cart.memory.usage` final=0.9506580056053604 components={'counterfactual_norm': 0.996654584360997, 'evidence_norm': 0.9094071604580067, 'metric_score_norm': 0.9542032796054852, 'service_score_norm': 0.9270364906119661, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 14: `checkoutservice.memory.usage` final=0.663539498789629 components={'counterfactual_norm': 0.0, 'evidence_norm': 0.43297756960592526, 'metric_score_norm': 0.7900742440102343, 'service_score_norm': 0.9285045381170379, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 21: `paymentservice.memory.usage` final=0.2832004638353362 components={'counterfactual_norm': 0.0, 'evidence_norm': 0.8320046383533617, 'metric_score_norm': 0.0, 'service_score_norm': 1.0, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
#### cpu
- rank 4: `adservice.cpu.usage` final=0.9210544872515181 components={'counterfactual_norm': 0.9898322739608637, 'evidence_norm': 0.8661393919704569, 'metric_score_norm': 0.9095722242680058, 'service_score_norm': 0.9285049180646978, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 5: `redis-cart.cpu.usage` final=0.9102607276971638 components={'counterfactual_norm': 0.9951098028354854, 'evidence_norm': 0.8320046383533617, 'metric_score_norm': 0.8952481732983847, 'service_score_norm': 0.9270364906119661, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 6: `recommendationservice.cpu.usage` final=0.9072717499386397 components={'counterfactual_norm': 0.9878308184803521, 'evidence_norm': 0.8320046383533617, 'metric_score_norm': 0.8923283835158998, 'service_score_norm': 0.9255802619875287, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 7: `currencyservice.cpu.throttled_usec` final=0.8412540941536077 components={'counterfactual_norm': 0.018295738423372215, 'evidence_norm': 0.9042150673188653, 'metric_score_norm': 1.0, 'service_score_norm': 0.9904411332910764, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 8: `currencyservice.cpu.usage` final=0.8326194211094571 components={'counterfactual_norm': 0.018295738423359836, 'evidence_norm': 0.8753308957326636, 'metric_score_norm': 0.9895522620263119, 'service_score_norm': 0.9904411332910764, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}

### Post-hoc Labels For Diagnosis Only

- incident_id: ob-cpu-paymentservice-repeat-03
- root_service: paymentservice
- root_metric: cpu.throttled_usec
- root_type: CPU throttling

## CPU Repeat 04

- top1_service: emailservice
- top1_metric: emailservice.memory.usage
- predicted_root_type: memory

### Top 10 Metric Candidates

- rank 1: `emailservice.memory.usage` family=memory final=0.9357354077422445 metric_score=8.525621203276833 service_score=8.271980881132922 evidence=2.23606797749979 cf_delta=19.306173554206453 components={'counterfactual_norm': 1.0, 'evidence_norm': 0.8901777160306067, 'metric_score_norm': 0.9214064404119747, 'service_score_norm': 0.9497204695629884, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 2: `shippingservice.memory.usage` family=memory final=0.9322349658708234 metric_score=8.525620766406693 service_score=8.157314415866892 evidence=2.23606797749979 cf_delta=19.19453177543687 components={'counterfactual_norm': 0.9942173016078965, 'evidence_norm': 0.8901777160306067, 'metric_score_norm': 0.9214063931972379, 'service_score_norm': 0.9365554138404869, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 3: `redis-cart.memory.usage` family=memory final=0.9302957379753642 metric_score=8.525621054578412 service_score=8.157313960712779 evidence=2.23606797749979 cf_delta=18.944937113868946 components={'counterfactual_norm': 0.9812890711189738, 'evidence_norm': 0.8901777160306067, 'metric_score_norm': 0.9214064243413941, 'service_score_norm': 0.9365553615834539, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 4: `adservice.memory.usage` family=memory final=0.9298126865705749 metric_score=8.525621392449882 service_score=8.144701797556012 evidence=2.23606797749979 cf_delta=18.92003644577676 components={'counterfactual_norm': 0.9799992936277339, 'evidence_norm': 0.8901777160306067, 'metric_score_norm': 0.9214064608568504, 'service_score_norm': 0.9351073372604323, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 5: `emailservice.cpu.usage` family=CPU final=0.9203754103637862 metric_score=8.403949195877694 service_score=8.271980881132922 evidence=2.23606797749979 cf_delta=18.26007990160042 components={'counterfactual_norm': 0.9458155884868181, 'evidence_norm': 0.8901777160306067, 'metric_score_norm': 0.9082567392274639, 'service_score_norm': 0.9497204695629884, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 6: `checkoutservice.memory.usage` family=memory final=0.9196178426369397 metric_score=8.525621477046878 service_score=8.144706619774924 evidence=2.23606797749979 cf_delta=17.6078653712093 components={'counterfactual_norm': 0.9120328956834057, 'evidence_norm': 0.8901777160306067, 'metric_score_norm': 0.92140646999967, 'service_score_norm': 0.9351078909077479, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 7: `shippingservice.cpu.usage` family=CPU final=0.9055143937602831 metric_score=8.165825171528867 service_score=8.157314415866892 evidence=2.23606797749979 cf_delta=18.50802047936918 components={'counterfactual_norm': 0.9586581425575462, 'evidence_norm': 0.8901777160306067, 'metric_score_norm': 0.8825214872827145, 'service_score_norm': 0.9365554138404869, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 8: `redis-cart.cpu.usage` family=CPU final=0.9052818237491737 metric_score=8.165823918366302 service_score=8.157313960712779 evidence=2.23606797749979 cf_delta=18.47809783194748 components={'counterfactual_norm': 0.9571082420898185, 'evidence_norm': 0.8901777160306067, 'metric_score_norm': 0.8825213518471807, 'service_score_norm': 0.9365553615834539, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 9: `currencyservice.cpu.throttled_usec` family=CPU final=0.8538170508332164 metric_score=9.252834394629257 service_score=8.709796499396498 evidence=2.511934344380856 cf_delta=-0.49162306671996703 components={'counterfactual_norm': 0.025464552327763137, 'evidence_norm': 1.0, 'metric_score_norm': 1.0, 'service_score_norm': 0.9999868399202596, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 10: `currencyservice.cpu.usage` family=CPU final=0.8413449354115689 metric_score=9.116914899554974 service_score=8.709796499396498 evidence=2.4015877976284297 cf_delta=-0.49162306671996703 components={'counterfactual_norm': 0.025464552327763137, 'evidence_norm': 0.9560710864122427, 'metric_score_norm': 0.9853105017038696, 'service_score_norm': 0.9999868399202596, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}

### Top 10 Services

- rank 1: `emailservice` final=0.9357354077422445 top_metric=emailservice.memory.usage a8r_service=8.271980881132922
- rank 2: `shippingservice` final=0.9322349658708234 top_metric=shippingservice.memory.usage a8r_service=8.157314415866892
- rank 3: `redis-cart` final=0.9302957379753642 top_metric=redis-cart.memory.usage a8r_service=8.157313960712779
- rank 4: `adservice` final=0.9298126865705749 top_metric=adservice.memory.usage a8r_service=8.144701797556012
- rank 5: `checkoutservice` final=0.9196178426369397 top_metric=checkoutservice.memory.usage a8r_service=8.144706619774924

### Memory vs CPU Candidate Components

#### memory
- rank 1: `emailservice.memory.usage` final=0.9357354077422445 components={'counterfactual_norm': 1.0, 'evidence_norm': 0.8901777160306067, 'metric_score_norm': 0.9214064404119747, 'service_score_norm': 0.9497204695629884, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 2: `shippingservice.memory.usage` final=0.9322349658708234 components={'counterfactual_norm': 0.9942173016078965, 'evidence_norm': 0.8901777160306067, 'metric_score_norm': 0.9214063931972379, 'service_score_norm': 0.9365554138404869, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 3: `redis-cart.memory.usage` final=0.9302957379753642 components={'counterfactual_norm': 0.9812890711189738, 'evidence_norm': 0.8901777160306067, 'metric_score_norm': 0.9214064243413941, 'service_score_norm': 0.9365553615834539, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 4: `adservice.memory.usage` final=0.9298126865705749 components={'counterfactual_norm': 0.9799992936277339, 'evidence_norm': 0.8901777160306067, 'metric_score_norm': 0.9214064608568504, 'service_score_norm': 0.9351073372604323, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 6: `checkoutservice.memory.usage` final=0.9196178426369397 components={'counterfactual_norm': 0.9120328956834057, 'evidence_norm': 0.8901777160306067, 'metric_score_norm': 0.92140646999967, 'service_score_norm': 0.9351078909077479, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
#### cpu
- rank 5: `emailservice.cpu.usage` final=0.9203754103637862 components={'counterfactual_norm': 0.9458155884868181, 'evidence_norm': 0.8901777160306067, 'metric_score_norm': 0.9082567392274639, 'service_score_norm': 0.9497204695629884, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 7: `shippingservice.cpu.usage` final=0.9055143937602831 components={'counterfactual_norm': 0.9586581425575462, 'evidence_norm': 0.8901777160306067, 'metric_score_norm': 0.8825214872827145, 'service_score_norm': 0.9365554138404869, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 8: `redis-cart.cpu.usage` final=0.9052818237491737 components={'counterfactual_norm': 0.9571082420898185, 'evidence_norm': 0.8901777160306067, 'metric_score_norm': 0.8825213518471807, 'service_score_norm': 0.9365553615834539, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 9: `currencyservice.cpu.throttled_usec` final=0.8538170508332164 components={'counterfactual_norm': 0.025464552327763137, 'evidence_norm': 1.0, 'metric_score_norm': 1.0, 'service_score_norm': 0.9999868399202596, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 10: `currencyservice.cpu.usage` final=0.8413449354115689 components={'counterfactual_norm': 0.025464552327763137, 'evidence_norm': 0.9560710864122427, 'metric_score_norm': 0.9853105017038696, 'service_score_norm': 0.9999868399202596, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}

### Post-hoc Labels For Diagnosis Only

- incident_id: ob-cpu-paymentservice-repeat-04
- root_service: paymentservice
- root_metric: cpu.throttled_usec
- root_type: CPU throttling

## CPU Repeat 05

- top1_service: productcatalogservice
- top1_metric: productcatalogservice.memory.usage
- predicted_root_type: memory

### Top 10 Metric Candidates

- rank 1: `productcatalogservice.memory.usage` family=memory final=0.9813209732985226 metric_score=8.93824494523235 service_score=8.11914156669772 evidence=2.671368096070459 cf_delta=18.82954523347007 components={'counterfactual_norm': 0.9951432525314103, 'evidence_norm': 0.9380530973451389, 'metric_score_norm': 1.0, 'service_score_norm': 0.9412208784214862, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 2: `redis-cart.memory.usage` family=memory final=0.9468350841693098 metric_score=8.580544846820317 service_score=8.110661371443907 evidence=2.3643669598153587 cf_delta=18.640529539551608 components={'counterfactual_norm': 0.9851537551700428, 'evidence_norm': 0.8302493966210818, 'metric_score_norm': 0.9599809469751855, 'service_score_norm': 0.9402378019767154, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 3: `emailservice.memory.usage` family=memory final=0.9379337911406719 metric_score=8.431057656832163 service_score=8.221742623431426 evidence=2.23606797749979 cf_delta=18.92144189851274 components={'counterfactual_norm': 1.0, 'evidence_norm': 0.7851971037811758, 'metric_score_norm': 0.9432565015271012, 'service_score_norm': 0.9531150246132429, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 4: `recommendationservice.memory.usage` family=memory final=0.9333416722901743 metric_score=8.431057909197127 service_score=8.110662814826952 evidence=2.23606797749979 cf_delta=18.667046478031807 components={'counterfactual_norm': 0.986555177885205, 'evidence_norm': 0.7851971037811758, 'metric_score_norm': 0.9432565297613872, 'service_score_norm': 0.9402379693025646, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 5: `paymentservice.cpu.throttle_ratio` family=CPU final=0.8243034559351581 metric_score=8.82733177370534 service_score=8.626029824319563 evidence=2.23606797749979 cf_delta=-0.32949867416346024 components={'counterfactual_norm': 0.017414036199289835, 'evidence_norm': 0.7851971037811758, 'metric_score_norm': 0.9875911689367866, 'service_score_norm': 0.9999824860595723, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 6: `adservice.cpu.throttle_ratio` family=CPU final=0.8242513995356898 metric_score=8.827468928197897 service_score=8.626180902738012 evidence=2.23606797749979 cf_delta=-0.32142568196468346 components={'counterfactual_norm': 0.016987377795449517, 'evidence_norm': 0.7851971037811758, 'metric_score_norm': 0.9876065136150088, 'service_score_norm': 1.0, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 7: `adservice.cpu.throttled_periods` family=CPU final=0.8242513995356896 metric_score=8.827468928197893 service_score=8.626180902738012 evidence=2.23606797749979 cf_delta=-0.32142568196468346 components={'counterfactual_norm': 0.016987377795449517, 'evidence_norm': 0.7851971037811758, 'metric_score_norm': 0.9876065136150084, 'service_score_norm': 1.0, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 8: `paymentservice.cpu.throttled_periods` family=CPU final=0.8227321259110162 metric_score=8.827296875126082 service_score=8.626029824319563 evidence=2.23606797749979 cf_delta=0.1315573584233789 components={'counterfactual_norm': 0.0069528188775993625, 'evidence_norm': 0.7851971037811758, 'metric_score_norm': 0.9875872645260805, 'service_score_norm': 0.9999824860595723, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 9: `frontend.cpu.throttled_periods` family=CPU final=0.8195263316705973 metric_score=8.789681456084951 service_score=8.589255078024863 evidence=2.23606797749979 cf_delta=0.12669314203503745 components={'counterfactual_norm': 0.006695744579856558, 'evidence_norm': 0.7851971037811758, 'metric_score_norm': 0.9833788970812841, 'service_score_norm': 0.9957193310539745, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 10: `frontend.cpu.throttled_usec` family=CPU final=0.8195263316705973 metric_score=8.789681456084951 service_score=8.589255078024863 evidence=2.23606797749979 cf_delta=0.12669314203503745 components={'counterfactual_norm': 0.006695744579856558, 'evidence_norm': 0.7851971037811758, 'metric_score_norm': 0.9833788970812841, 'service_score_norm': 0.9957193310539745, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}

### Top 10 Services

- rank 1: `productcatalogservice` final=0.9813209732985226 top_metric=productcatalogservice.memory.usage a8r_service=8.11914156669772
- rank 2: `redis-cart` final=0.9468350841693098 top_metric=redis-cart.memory.usage a8r_service=8.110661371443907
- rank 3: `emailservice` final=0.9379337911406719 top_metric=emailservice.memory.usage a8r_service=8.221742623431426
- rank 4: `recommendationservice` final=0.9333416722901743 top_metric=recommendationservice.memory.usage a8r_service=8.110662814826952
- rank 5: `paymentservice` final=0.8243034559351581 top_metric=paymentservice.cpu.throttle_ratio a8r_service=8.626029824319563

### Memory vs CPU Candidate Components

#### memory
- rank 1: `productcatalogservice.memory.usage` final=0.9813209732985226 components={'counterfactual_norm': 0.9951432525314103, 'evidence_norm': 0.9380530973451389, 'metric_score_norm': 1.0, 'service_score_norm': 0.9412208784214862, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 2: `redis-cart.memory.usage` final=0.9468350841693098 components={'counterfactual_norm': 0.9851537551700428, 'evidence_norm': 0.8302493966210818, 'metric_score_norm': 0.9599809469751855, 'service_score_norm': 0.9402378019767154, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 3: `emailservice.memory.usage` final=0.9379337911406719 components={'counterfactual_norm': 1.0, 'evidence_norm': 0.7851971037811758, 'metric_score_norm': 0.9432565015271012, 'service_score_norm': 0.9531150246132429, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 4: `recommendationservice.memory.usage` final=0.9333416722901743 components={'counterfactual_norm': 0.986555177885205, 'evidence_norm': 0.7851971037811758, 'metric_score_norm': 0.9432565297613872, 'service_score_norm': 0.9402379693025646, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 19: `shippingservice.memory.usage` final=0.652822556864415 components={'counterfactual_norm': 0.0, 'evidence_norm': 0.34947069983267065, 'metric_score_norm': 0.7815053979511932, 'service_score_norm': 0.9402375900399582, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
#### cpu
- rank 5: `paymentservice.cpu.throttle_ratio` final=0.8243034559351581 components={'counterfactual_norm': 0.017414036199289835, 'evidence_norm': 0.7851971037811758, 'metric_score_norm': 0.9875911689367866, 'service_score_norm': 0.9999824860595723, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 6: `adservice.cpu.throttle_ratio` final=0.8242513995356898 components={'counterfactual_norm': 0.016987377795449517, 'evidence_norm': 0.7851971037811758, 'metric_score_norm': 0.9876065136150088, 'service_score_norm': 1.0, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 7: `adservice.cpu.throttled_periods` final=0.8242513995356896 components={'counterfactual_norm': 0.016987377795449517, 'evidence_norm': 0.7851971037811758, 'metric_score_norm': 0.9876065136150084, 'service_score_norm': 1.0, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 8: `paymentservice.cpu.throttled_periods` final=0.8227321259110162 components={'counterfactual_norm': 0.0069528188775993625, 'evidence_norm': 0.7851971037811758, 'metric_score_norm': 0.9875872645260805, 'service_score_norm': 0.9999824860595723, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}
- rank 9: `frontend.cpu.throttled_periods` final=0.8195263316705973 components={'counterfactual_norm': 0.006695744579856558, 'evidence_norm': 0.7851971037811758, 'metric_score_norm': 0.9833788970812841, 'service_score_norm': 0.9957193310539745, 'weights': {'counterfactual': 0.15, 'evidence': 0.1, 'metric': 0.55, 'service': 0.2}}

### Post-hoc Labels For Diagnosis Only

- incident_id: ob-cpu-paymentservice-repeat-05
- root_service: paymentservice
- root_metric: cpu.throttled_usec
- root_type: CPU throttling

## Diagnosis Summary

- CPU repeats with memory.usage as top1: [1, 2, 3, 4, 5]
- In B2, `metric_score_norm` and A8R service support often let broad `memory.usage` candidates outrank more diagnostic CPU metrics.
- Diagnostic specificity is missing from the final candidate score: `memory.usage` receives no penalty for being a weak root-cause indicator, while `cpu.throttled_usec` / `cpu.throttle_ratio` receive no explicit semantic specificity boost.
- Evidence support exists in the candidate table, but broad memory usage can still win because the final score is dominated by metric/service score and counterfactual support.
- Request/load symptom penalty is not sufficient here because the false leaders are memory-family resource metrics, not request-family symptom metrics.
- `memory.usage` is treated as a strong root metric in B2 even without strong memory-specific evidence such as memory.events, memory.oom, memory.reclaim, or memory.pressure.
