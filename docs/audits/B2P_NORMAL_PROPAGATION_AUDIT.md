# B2P Normal Propagation Audit

Labels are used only for post-hoc diagnosis, not for propagation learning or repair.

## Summary

- B2M service_hit_at_1_overall: 0.75

- graph_direction_assumption: service_graph src->dst is caller->callee; integrated path explanation traverses callee->caller for impact paths.

## CPU Repeat Diagnosis

```json
[
  {
    "repeat": 1,
    "predicted_top1_service": "currencyservice",
    "true_root_service_debug_only": "paymentservice",
    "true_root_service_rank_debug_only": 3,
    "predicted_score_components": {
      "a8r_service_score_norm": 0.9969152049276732,
      "best_metric_score_norm": 0.9835944636086927,
      "cpu_service_diagnostic_boost_applied": true,
      "diagnostic_family_support": 1.0,
      "downstream_load_support": 0.9024164382706095,
      "final_service_score": 1.0007439024754947,
      "global_family_support_weight_limited": true,
      "local_family_support": 0.9439177085205641,
      "path_penalty_applied": false,
      "path_to_symptom_support": 0.3333333333333333,
      "service_counterfactual_norm": 0.9311695815368604,
      "service_local_support_used": true,
      "symptom_service_load_penalty_applied": false,
      "top2_metric_mean_norm": 0.9844215065612224,
      "weights": {
        "a8r_service": 0.15,
        "best_metric": 0.25,
        "downstream_load": 0.1,
        "path": 0.15,
        "service_counterfactual": 0.15,
        "service_local_family": 0.05,
        "top2": 0.15
      }
    },
    "true_score_components_debug_only": {
      "a8r_service_score_norm": 0.9999994467299325,
      "best_metric_score_norm": 0.8668914198903764,
      "cpu_service_diagnostic_boost_applied": true,
      "diagnostic_family_support": 1.0,
      "downstream_load_support": 0.9024164382706095,
      "final_service_score": 0.9206910938081817,
      "global_family_support_weight_limited": true,
      "local_family_support": 0.4990688245172732,
      "path_penalty_applied": false,
      "path_to_symptom_support": 0.3333333333333333,
      "service_counterfactual_norm": 0.9346423152944195,
      "service_local_support_used": true,
      "symptom_service_load_penalty_applied": false,
      "top2_metric_mean_norm": 0.8565783120558522,
      "weights": {
        "a8r_service": 0.15,
        "best_metric": 0.25,
        "downstream_load": 0.1,
        "path": 0.15,
        "service_counterfactual": 0.15,
        "service_local_family": 0.05,
        "top2": 0.15
      }
    },
    "a6_edge_counts": {
      "same_service_edge_count": 350,
      "self_lag_edge_count": 60,
      "cross_service_edge_count": 500,
      "request_to_request_edge_count": 25,
      "resource_to_request_edge_count": 50
    },
    "a6_average_abs_edge_weight_predicted_service": 32146260.93296588,
    "resource_to_request_parent_coverage_top_services": [
      {
        "service": "currencyservice",
        "best_metric": "currencyservice.cpu.throttled_usec",
        "rank": 1,
        "final_service_score": 1.0007439024754947
      },
      {
        "service": "frontend",
        "best_metric": "frontend.cpu.throttled_periods",
        "rank": 2,
        "final_service_score": 0.9827574555342897
      },
      {
        "service": "paymentservice",
        "best_metric": "paymentservice.cpu.throttled_usec",
        "rank": 3,
        "final_service_score": 0.9206910938081817
      },
      {
        "service": "emailservice",
        "best_metric": "emailservice.cpu.usage",
        "rank": 4,
        "final_service_score": 0.7886537997541945
      },
      {
        "service": "cartservice",
        "best_metric": "cartservice.cpu.usage",
        "rank": 5,
        "final_service_score": 0.6143673987873393
      },
      {
        "service": "recommendationservice",
        "best_metric": "recommendationservice.cpu.usage",
        "rank": 6,
        "final_service_score": 0.6079919629574229
      },
      {
        "service": "shippingservice",
        "best_metric": "shippingservice.cpu.usage",
        "rank": 7,
        "final_service_score": 0.5848974421260358
      },
      {
        "service": "redis-cart",
        "best_metric": "redis-cart.cpu.usage",
        "rank": 8,
        "final_service_score": 0.5848973692094708
      },
      {
        "service": "productcatalogservice",
        "best_metric": "productcatalogservice.cpu.usage",
        "rank": 9,
        "final_service_score": 0.5331643363897054
      },
      {
        "service": "checkoutservice",
        "best_metric": "checkoutservice.cpu.usage",
        "rank": 10,
        "final_service_score": 0.45365748949038276
      }
    ]
  },
  {
    "repeat": 2,
    "predicted_top1_service": "currencyservice",
    "true_root_service_debug_only": "paymentservice",
    "true_root_service_rank_debug_only": 3,
    "predicted_score_components": {
      "a8r_service_score_norm": 0.9904430857789018,
      "best_metric_score_norm": 1.0,
      "cpu_service_diagnostic_boost_applied": true,
      "diagnostic_family_support": 1.0,
      "downstream_load_support": 0.824608113280059,
      "final_service_score": 1.007837866950099,
      "global_family_support_weight_limited": true,
      "local_family_support": 1.0,
      "path_penalty_applied": false,
      "path_to_symptom_support": 0.3333333333333333,
      "service_counterfactual_norm": 0.9690231978320674,
      "service_local_support_used": true,
      "symptom_service_load_penalty_applied": false,
      "top2_metric_mean_norm": 1.0,
      "weights": {
        "a8r_service": 0.15,
        "best_metric": 0.25,
        "downstream_load": 0.1,
        "path": 0.15,
        "service_counterfactual": 0.15,
        "service_local_family": 0.05,
        "top2": 0.15
      }
    },
    "true_score_components_debug_only": {
      "a8r_service_score_norm": 1.0,
      "best_metric_score_norm": 0.7482675987596114,
      "cpu_service_diagnostic_boost_applied": true,
      "diagnostic_family_support": 1.0,
      "downstream_load_support": 0.824608113280059,
      "final_service_score": 0.8563111435320588,
      "global_family_support_weight_limited": true,
      "local_family_support": 0.34630249251614226,
      "path_penalty_applied": false,
      "path_to_symptom_support": 0.3333333333333333,
      "service_counterfactual_norm": 0.946996262669863,
      "service_local_support_used": true,
      "symptom_service_load_penalty_applied": false,
      "top2_metric_mean_norm": 0.7715074042390396,
      "weights": {
        "a8r_service": 0.15,
        "best_metric": 0.25,
        "downstream_load": 0.1,
        "path": 0.15,
        "service_counterfactual": 0.15,
        "service_local_family": 0.05,
        "top2": 0.15
      }
    },
    "a6_edge_counts": {
      "same_service_edge_count": 350,
      "self_lag_edge_count": 60,
      "cross_service_edge_count": 500,
      "request_to_request_edge_count": 25,
      "resource_to_request_edge_count": 50
    },
    "a6_average_abs_edge_weight_predicted_service": 83863577.68030761,
    "resource_to_request_parent_coverage_top_services": [
      {
        "service": "currencyservice",
        "best_metric": "currencyservice.cpu.throttled_usec",
        "rank": 1,
        "final_service_score": 1.007837866950099
      },
      {
        "service": "frontend",
        "best_metric": "frontend.cpu.throttled_usec",
        "rank": 2,
        "final_service_score": 0.9497978300316144
      },
      {
        "service": "paymentservice",
        "best_metric": "paymentservice.cpu.throttle_ratio",
        "rank": 3,
        "final_service_score": 0.8563111435320588
      },
      {
        "service": "emailservice",
        "best_metric": "emailservice.cpu.usage",
        "rank": 4,
        "final_service_score": 0.7407869783272557
      },
      {
        "service": "shippingservice",
        "best_metric": "shippingservice.cpu.usage",
        "rank": 5,
        "final_service_score": 0.7298719609700872
      },
      {
        "service": "checkoutservice",
        "best_metric": "checkoutservice.cpu.usage",
        "rank": 6,
        "final_service_score": 0.6107233135545179
      },
      {
        "service": "recommendationservice",
        "best_metric": "recommendationservice.cpu.usage",
        "rank": 7,
        "final_service_score": 0.562522190056305
      },
      {
        "service": "productcatalogservice",
        "best_metric": "productcatalogservice.cpu.usage",
        "rank": 8,
        "final_service_score": 0.49147766524207354
      },
      {
        "service": "cartservice",
        "best_metric": "cartservice.cpu.usage",
        "rank": 9,
        "final_service_score": 0.49147665405125857
      },
      {
        "service": "redis-cart",
        "best_metric": "redis-cart.cpu.usage",
        "rank": 10,
        "final_service_score": 0.46411470704209284
      }
    ]
  },
  {
    "repeat": 3,
    "predicted_top1_service": "currencyservice",
    "true_root_service_debug_only": "paymentservice",
    "true_root_service_rank_debug_only": 3,
    "predicted_score_components": {
      "a8r_service_score_norm": 0.9904411332910764,
      "best_metric_score_norm": 1.0,
      "cpu_service_diagnostic_boost_applied": true,
      "diagnostic_family_support": 1.0,
      "downstream_load_support": 1.0,
      "final_service_score": 1.0333510954927108,
      "global_family_support_weight_limited": true,
      "local_family_support": 1.0,
      "path_penalty_applied": false,
      "path_to_symptom_support": 0.3333333333333333,
      "service_counterfactual_norm": 1.0,
      "service_local_support_used": true,
      "symptom_service_load_penalty_applied": false,
      "top2_metric_mean_norm": 1.0,
      "weights": {
        "a8r_service": 0.15,
        "best_metric": 0.25,
        "downstream_load": 0.1,
        "path": 0.15,
        "service_counterfactual": 0.15,
        "service_local_family": 0.05,
        "top2": 0.15
      }
    },
    "true_score_components_debug_only": {
      "a8r_service_score_norm": 1.0,
      "best_metric_score_norm": 0.8107732570199524,
      "cpu_service_diagnostic_boost_applied": true,
      "diagnostic_family_support": 1.0,
      "downstream_load_support": 1.0,
      "final_service_score": 0.9151829363313472,
      "global_family_support_weight_limited": true,
      "local_family_support": 0.48698221206757275,
      "path_penalty_applied": false,
      "path_to_symptom_support": 0.3333333333333333,
      "service_counterfactual_norm": 0.9766171005081681,
      "service_local_support_used": true,
      "symptom_service_load_penalty_applied": false,
      "top2_metric_mean_norm": 0.8151750603279213,
      "weights": {
        "a8r_service": 0.15,
        "best_metric": 0.25,
        "downstream_load": 0.1,
        "path": 0.15,
        "service_counterfactual": 0.15,
        "service_local_family": 0.05,
        "top2": 0.15
      }
    },
    "a6_edge_counts": {
      "same_service_edge_count": 350,
      "self_lag_edge_count": 60,
      "cross_service_edge_count": 500,
      "request_to_request_edge_count": 25,
      "resource_to_request_edge_count": 50
    },
    "a6_average_abs_edge_weight_predicted_service": 137236312.6716143,
    "resource_to_request_parent_coverage_top_services": [
      {
        "service": "currencyservice",
        "best_metric": "currencyservice.cpu.throttled_usec",
        "rank": 1,
        "final_service_score": 1.0333510954927108
      },
      {
        "service": "frontend",
        "best_metric": "frontend.cpu.throttled_usec",
        "rank": 2,
        "final_service_score": 0.9903521607755976
      },
      {
        "service": "paymentservice",
        "best_metric": "paymentservice.cpu.throttled_periods",
        "rank": 3,
        "final_service_score": 0.9151829363313472
      },
      {
        "service": "checkoutservice",
        "best_metric": "checkoutservice.cpu.usage",
        "rank": 4,
        "final_service_score": 0.687722248772479
      },
      {
        "service": "recommendationservice",
        "best_metric": "recommendationservice.cpu.usage",
        "rank": 5,
        "final_service_score": 0.6782924375291849
      },
      {
        "service": "redis-cart",
        "best_metric": "redis-cart.cpu.usage",
        "rank": 6,
        "final_service_score": 0.6542868615645198
      },
      {
        "service": "productcatalogservice",
        "best_metric": "productcatalogservice.cpu.usage",
        "rank": 7,
        "final_service_score": 0.5918044277189887
      },
      {
        "service": "emailservice",
        "best_metric": "emailservice.cpu.usage",
        "rank": 8,
        "final_service_score": 0.571540479392581
      },
      {
        "service": "cartservice",
        "best_metric": "cartservice.cpu.usage",
        "rank": 9,
        "final_service_score": 0.5568351207969646
      },
      {
        "service": "shippingservice",
        "best_metric": "shippingservice.cpu.usage",
        "rank": 10,
        "final_service_score": 0.5292732413497527
      }
    ]
  },
  {
    "repeat": 4,
    "predicted_top1_service": "currencyservice",
    "true_root_service_debug_only": "paymentservice",
    "true_root_service_rank_debug_only": 3,
    "predicted_score_components": {
      "a8r_service_score_norm": 0.9999868399202596,
      "best_metric_score_norm": 1.0,
      "cpu_service_diagnostic_boost_applied": true,
      "diagnostic_family_support": 1.0,
      "downstream_load_support": 0.856695207186652,
      "final_service_score": 1.0120970719122553,
      "global_family_support_weight_limited": true,
      "local_family_support": 1.0,
      "path_penalty_applied": false,
      "path_to_symptom_support": 0.3333333333333333,
      "service_counterfactual_norm": 0.9627790910118577,
      "service_local_support_used": true,
      "symptom_service_load_penalty_applied": false,
      "top2_metric_mean_norm": 1.0,
      "weights": {
        "a8r_service": 0.15,
        "best_metric": 0.25,
        "downstream_load": 0.1,
        "path": 0.15,
        "service_counterfactual": 0.15,
        "service_local_family": 0.05,
        "top2": 0.15
      }
    },
    "true_score_components_debug_only": {
      "a8r_service_score_norm": 1.0,
      "best_metric_score_norm": 0.8411152356964465,
      "cpu_service_diagnostic_boost_applied": true,
      "diagnostic_family_support": 1.0,
      "downstream_load_support": 0.856695207186652,
      "final_service_score": 0.9031618406900974,
      "global_family_support_weight_limited": true,
      "local_family_support": 0.4768842483199293,
      "path_penalty_applied": false,
      "path_to_symptom_support": 0.3333333333333333,
      "service_counterfactual_norm": 0.9435665538224903,
      "service_local_support_used": true,
      "symptom_service_load_penalty_applied": false,
      "top2_metric_mean_norm": 0.8268706480471227,
      "weights": {
        "a8r_service": 0.15,
        "best_metric": 0.25,
        "downstream_load": 0.1,
        "path": 0.15,
        "service_counterfactual": 0.15,
        "service_local_family": 0.05,
        "top2": 0.15
      }
    },
    "a6_edge_counts": {
      "same_service_edge_count": 350,
      "self_lag_edge_count": 60,
      "cross_service_edge_count": 500,
      "request_to_request_edge_count": 25,
      "resource_to_request_edge_count": 50
    },
    "a6_average_abs_edge_weight_predicted_service": 72960608035801.73,
    "resource_to_request_parent_coverage_top_services": [
      {
        "service": "currencyservice",
        "best_metric": "currencyservice.cpu.throttled_usec",
        "rank": 1,
        "final_service_score": 1.0120970719122553
      },
      {
        "service": "frontend",
        "best_metric": "frontend.cpu.throttle_ratio",
        "rank": 2,
        "final_service_score": 0.963745621644
      },
      {
        "service": "paymentservice",
        "best_metric": "paymentservice.cpu.throttled_usec",
        "rank": 3,
        "final_service_score": 0.9031618406900974
      },
      {
        "service": "emailservice",
        "best_metric": "emailservice.cpu.usage",
        "rank": 4,
        "final_service_score": 0.7900267761407099
      },
      {
        "service": "shippingservice",
        "best_metric": "shippingservice.cpu.usage",
        "rank": 5,
        "final_service_score": 0.7814011069724504
      },
      {
        "service": "redis-cart",
        "best_metric": "redis-cart.cpu.usage",
        "rank": 6,
        "final_service_score": 0.6349085866363352
      },
      {
        "service": "productcatalogservice",
        "best_metric": "productcatalogservice.cpu.usage",
        "rank": 7,
        "final_service_score": 0.5525076849711391
      },
      {
        "service": "cartservice",
        "best_metric": "cartservice.cpu.usage",
        "rank": 8,
        "final_service_score": 0.5378444264883109
      },
      {
        "service": "recommendationservice",
        "best_metric": "recommendationservice.cpu.usage",
        "rank": 9,
        "final_service_score": 0.5345017796979663
      },
      {
        "service": "checkoutservice",
        "best_metric": "checkoutservice.cpu.usage",
        "rank": 10,
        "final_service_score": 0.5226506926823846
      }
    ]
  },
  {
    "repeat": 5,
    "predicted_top1_service": "frontend",
    "true_root_service_debug_only": "paymentservice",
    "true_root_service_rank_debug_only": 2,
    "predicted_score_components": {
      "a8r_service_score_norm": 0.9957193310539745,
      "best_metric_score_norm": 1.0,
      "cpu_service_diagnostic_boost_applied": true,
      "diagnostic_family_support": 1.0,
      "downstream_load_support": 0.9300698505443137,
      "final_service_score": 1.1192446965528549,
      "global_family_support_weight_limited": true,
      "local_family_support": 1.0,
      "path_penalty_applied": false,
      "path_to_symptom_support": 1.0,
      "service_counterfactual_norm": 0.8726091543968009,
      "service_local_support_used": true,
      "symptom_service_load_penalty_applied": false,
      "top2_metric_mean_norm": 1.0,
      "weights": {
        "a8r_service": 0.15,
        "best_metric": 0.25,
        "downstream_load": 0.1,
        "path": 0.15,
        "service_counterfactual": 0.15,
        "service_local_family": 0.05,
        "top2": 0.15
      }
    },
    "true_score_components_debug_only": {
      "a8r_service_score_norm": 0.9999824860595723,
      "best_metric_score_norm": 0.9799606080558524,
      "cpu_service_diagnostic_boost_applied": true,
      "diagnostic_family_support": 1.0,
      "downstream_load_support": 0.9300698505443137,
      "final_service_score": 1.0058872602059366,
      "global_family_support_weight_limited": true,
      "local_family_support": 1.0,
      "path_penalty_applied": false,
      "path_to_symptom_support": 0.3333333333333333,
      "service_counterfactual_norm": 0.9200712163062812,
      "service_local_support_used": true,
      "symptom_service_load_penalty_applied": false,
      "top2_metric_mean_norm": 0.9911958777923081,
      "weights": {
        "a8r_service": 0.15,
        "best_metric": 0.25,
        "downstream_load": 0.1,
        "path": 0.15,
        "service_counterfactual": 0.15,
        "service_local_family": 0.05,
        "top2": 0.15
      }
    },
    "a6_edge_counts": {
      "same_service_edge_count": 350,
      "self_lag_edge_count": 60,
      "cross_service_edge_count": 500,
      "request_to_request_edge_count": 25,
      "resource_to_request_edge_count": 50
    },
    "a6_average_abs_edge_weight_predicted_service": 4279585187.389859,
    "resource_to_request_parent_coverage_top_services": [
      {
        "service": "frontend",
        "best_metric": "frontend.cpu.throttled_usec",
        "rank": 1,
        "final_service_score": 1.1192446965528549
      },
      {
        "service": "paymentservice",
        "best_metric": "paymentservice.cpu.throttle_ratio",
        "rank": 2,
        "final_service_score": 1.0058872602059366
      },
      {
        "service": "currencyservice",
        "best_metric": "currencyservice.cpu.throttle_ratio",
        "rank": 3,
        "final_service_score": 0.9169451002035438
      },
      {
        "service": "emailservice",
        "best_metric": "emailservice.cpu.usage",
        "rank": 4,
        "final_service_score": 0.7535820482365649
      },
      {
        "service": "productcatalogservice",
        "best_metric": "productcatalogservice.cpu.usage",
        "rank": 5,
        "final_service_score": 0.6233964832467025
      },
      {
        "service": "recommendationservice",
        "best_metric": "recommendationservice.cpu.usage",
        "rank": 6,
        "final_service_score": 0.622966387227147
      },
      {
        "service": "redis-cart",
        "best_metric": "redis-cart.cpu.usage",
        "rank": 7,
        "final_service_score": 0.5979663113584944
      },
      {
        "service": "shippingservice",
        "best_metric": "shippingservice.cpu.usage",
        "rank": 8,
        "final_service_score": 0.5979662188739654
      },
      {
        "service": "cartservice",
        "best_metric": "cartservice.cpu.usage",
        "rank": 9,
        "final_service_score": 0.5542051473956415
      },
      {
        "service": "checkoutservice",
        "best_metric": "checkoutservice.cpu.usage",
        "rank": 10,
        "final_service_score": 0.5348708280234307
      }
    ]
  }
]
```

## A6 Edge Summary

```json
{
  "same_service_edge_count": 1750,
  "self_lag_edge_count": 300,
  "cross_service_edge_count": 2500,
  "request_to_request_edge_count": 125,
  "resource_to_request_edge_count": 250
}
```

## Audit Answers

- graph_direction_assumption: src->dst is caller->callee; impact/path explanation uses reverse callee->caller traversal.

- multi_lag_missing: true for A6 IPW-RLS edges; A6 edge output has no lag field and effectively uses lag=1 parent features.

- propagation_not_used_in_service_score: true for B2M; service_candidate_table has path support and downstream load support but no structured propagation support.

- cross_service_edges_too_weak: likely; A6 edges include cross-service edges, but B2M final scoring did not consume path-specific learned edge support.

## Conclusion

- propagation_learning_status: insufficient_for_cpu

- likely_failure_causes: single_lag_too_simple, propagation_not_used_in_service_score, cross_service_edges_too_weak
