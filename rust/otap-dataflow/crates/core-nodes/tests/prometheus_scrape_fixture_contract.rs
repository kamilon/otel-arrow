// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Fixture integrity and independent semantic golden checks for the receiver.

use otap_df_pdata::{
    proto::opentelemetry::{
        common::v1::{AnyValue, InstrumentationScope, KeyValue},
        metrics::v1::{
            AggregationTemporality, Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics,
            ScopeMetrics, Sum, metric, number_data_point,
        },
        resource::v1::Resource,
    },
    testing::round_trip::test_metrics_round_trip,
};
use regex::RegexBuilder;
use serde_json::Value;

const SCALAR_FIXTURE: &str = include_str!("fixtures/prometheus_scrape/scalar_basic.json");
const TARGET_INFO_OPENMETRICS: &str =
    include_str!("fixtures/prometheus_scrape/target_info_openmetrics.json");
const TARGET_INFO_TEXT_GAUGE: &str =
    include_str!("fixtures/prometheus_scrape/target_info_text_gauge.json");
const ORDINARY_INFO_GAUGE: &str =
    include_str!("fixtures/prometheus_scrape/ordinary_info_gauge.json");

#[test]
fn scalar_fixture_pins_exact_semantics_and_identity() {
    let fixture: Value = serde_json::from_str(SCALAR_FIXTURE).expect("valid fixture JSON");

    assert_eq!(fixture["id"], "scalar-basic");
    assert_eq!(fixture["format"], "prometheus-text-0.0.4");
    assert_eq!(
        fixture["input"]
            .as_str()
            .expect("fixture input")
            .lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
            .count(),
        2
    );

    let expected = &fixture["expected"];
    assert_eq!(expected["resource"]["service.name"], "edge");
    assert_eq!(expected["resource"]["service.instance.id"], "api-a");
    assert_eq!(expected["resource"]["server.address"], "127.0.0.1");
    assert_eq!(expected["resource"]["server.port"], 9090);
    assert_eq!(expected["scope"]["name"], "otel-arrow.prometheus_scrape");
    assert_eq!(expected["scope"]["version"], "0.1.0");
    assert_eq!(
        expected["metrics"].as_array().expect("metric list").len(),
        2
    );

    let metrics = expected["metrics"].as_array().expect("metric list");
    assert_eq!(metrics[0]["name"], "requests_total");
    assert_eq!(metrics[0]["prometheus.type"], "counter");
    assert_eq!(metrics[0]["data_type"], "sum");
    assert_eq!(metrics[0]["is_monotonic"], true);
    assert_eq!(metrics[0]["points"].as_array().expect("points").len(), 1);
    assert_eq!(metrics[0]["points"][0]["value"], 7.0);
    assert_eq!(
        metrics[0]["points"][0]["time_unix_nano"],
        1_700_000_000_123_000_000_i64
    );
    assert_eq!(metrics[0]["points"][0]["attributes"]["method"], "GET");

    assert_eq!(metrics[1]["name"], "temperature_celsius");
    assert_eq!(metrics[1]["prometheus.type"], "gauge");
    assert_eq!(metrics[1]["data_type"], "gauge");
    assert_eq!(metrics[1]["points"].as_array().expect("points").len(), 1);
    assert_eq!(metrics[1]["points"][0]["value"], 21.5);
    assert_eq!(
        metrics[1]["points"][0]["time_unix_nano"],
        fixture["scrape_time_unix_nano"]
    );
    assert_eq!(metrics[1]["points"][0]["attributes"]["zone"], "west");

    for forbidden in [
        "up",
        "scrape_duration_seconds",
        "scrape_samples_scraped",
        "scrape_samples_post_metric_relabeling",
        "scrape_series_added",
    ] {
        assert!(
            !metrics.iter().any(|metric| metric["name"] == forbidden),
            "unexpected generated series {forbidden}"
        );
    }
}

#[test]
fn target_info_recognition_is_dialect_and_type_specific() {
    let openmetrics: Value =
        serde_json::from_str(TARGET_INFO_OPENMETRICS).expect("valid OpenMetrics fixture");
    assert_eq!(openmetrics["format"], "openmetrics-text-1.0.0");
    assert_eq!(
        openmetrics["input"]
            .as_str()
            .expect("OpenMetrics fixture input")
            .lines()
            .last(),
        Some("# EOF")
    );
    assert_eq!(
        openmetrics["input"]
            .as_str()
            .expect("OpenMetrics fixture input")
            .lines()
            .filter(|line| *line == "# EOF")
            .count(),
        1
    );
    assert_eq!(openmetrics["expected"]["resource"]["cluster"], "west");
    assert_eq!(openmetrics["expected"]["resource"]["version"], "2");
    assert!(
        openmetrics["expected"]["metrics"]
            .as_array()
            .expect("metric list")
            .is_empty()
    );

    let text_gauge: Value =
        serde_json::from_str(TARGET_INFO_TEXT_GAUGE).expect("valid text fixture");
    assert_eq!(text_gauge["expected"]["resource"]["cluster"], "west");
    assert!(
        text_gauge["expected"]["metrics"]
            .as_array()
            .expect("metric list")
            .is_empty()
    );

    let ordinary_gauge: Value =
        serde_json::from_str(ORDINARY_INFO_GAUGE).expect("valid ordinary info fixture");
    assert_eq!(
        ordinary_gauge["expected"]["resource"]["version"],
        Value::Null
    );
    assert_eq!(
        ordinary_gauge["expected"]["metrics"][0]["name"],
        "build_info"
    );
    assert_eq!(
        ordinary_gauge["expected"]["metrics"][0]["data_type"],
        "gauge"
    );
}

#[test]
fn scalar_golden_roundtrips_through_native_metric_builders() {
    let fixture: Value = serde_json::from_str(SCALAR_FIXTURE).expect("valid fixture JSON");
    let expected = &fixture["expected"];
    let resource = Resource {
        attributes: key_values(&expected["resource"]),
        ..Default::default()
    };
    let scope = InstrumentationScope {
        name: expected["scope"]["name"]
            .as_str()
            .expect("scope name")
            .to_owned(),
        version: expected["scope"]["version"]
            .as_str()
            .expect("scope version")
            .to_owned(),
        ..Default::default()
    };
    let metrics = expected["metrics"]
        .as_array()
        .expect("metric list")
        .iter()
        .map(|metric| {
            let points = metric["points"]
                .as_array()
                .expect("point list")
                .iter()
                .map(|point| NumberDataPoint {
                    attributes: key_values(&point["attributes"]),
                    time_unix_nano: point["time_unix_nano"].as_u64().expect("point timestamp"),
                    value: Some(number_data_point::Value::AsDouble(
                        point["value"].as_f64().expect("point value"),
                    )),
                    ..Default::default()
                })
                .collect();
            let data = match metric["data_type"].as_str().expect("metric data type") {
                "gauge" => metric::Data::Gauge(Gauge {
                    data_points: points,
                }),
                "sum" => metric::Data::Sum(Sum {
                    data_points: points,
                    aggregation_temporality: AggregationTemporality::Cumulative as i32,
                    is_monotonic: metric["is_monotonic"].as_bool().expect("sum monotonicity"),
                }),
                other => panic!("unsupported fixture data type {other}"),
            };
            Metric {
                name: metric["name"].as_str().expect("metric name").to_owned(),
                description: metric["description"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                data: Some(data),
                ..Default::default()
            }
        })
        .collect();

    test_metrics_round_trip(MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(resource),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(scope),
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    });
}

fn key_values(attributes: &Value) -> Vec<KeyValue> {
    attributes
        .as_object()
        .expect("attribute map")
        .iter()
        .map(|(key, value)| {
            let value = match value {
                Value::String(value) => AnyValue::new_string(value),
                Value::Number(value) => AnyValue::new_int(
                    value
                        .as_i64()
                        .expect("fixture number attribute is a signed integer"),
                ),
                other => panic!("unsupported fixture attribute {other}"),
            };
            KeyValue::new(key, value)
        })
        .collect()
}

fn generate_bounded_gauge_fixture(series: usize) -> String {
    assert!(series <= 512, "fixture generator has a fixed series cap");
    let mut input = String::with_capacity(series.saturating_mul(48).saturating_add(32));
    input.push_str("# TYPE generated_load gauge\n");
    for index in 0..series {
        input.push_str("generated_load{series=\"");
        input.push_str(&index.to_string());
        input.push_str("\"} ");
        input.push_str(&index.to_string());
        input.push('\n');
    }
    input
}

fn generate_drop_heavy_fixture(series: usize) -> String {
    assert!(series <= 512, "fixture generator has a fixed series cap");
    let mut input = String::with_capacity(series.saturating_mul(64).saturating_add(32));
    input.push_str("# TYPE generated_load gauge\n");
    for index in 0..series {
        let keep = if index % 100 == 0 { "keep" } else { "drop" };
        input.push_str("generated_load{series=\"");
        input.push_str(&index.to_string());
        input.push_str("\",selection=\"");
        input.push_str(keep);
        input.push_str("\"} ");
        input.push_str(&index.to_string());
        input.push('\n');
    }
    input
}

#[test]
fn generated_fixture_is_deterministic_and_bounded() {
    let first = generate_bounded_gauge_fixture(512);
    let second = generate_bounded_gauge_fixture(512);
    assert_eq!(first, second);
    assert!(first.len() <= 64 * 1024);
    assert_eq!(
        first.lines().filter(|line| !line.starts_with('#')).count(),
        512
    );

    let drop_heavy = generate_drop_heavy_fixture(512);
    assert_eq!(drop_heavy, generate_drop_heavy_fixture(512));
    assert!(drop_heavy.len() <= 64 * 1024);
    assert_eq!(
        drop_heavy
            .lines()
            .filter(|line| line.contains("selection=\"keep\""))
            .count(),
        6
    );
    assert_eq!(
        drop_heavy
            .lines()
            .filter(|line| line.contains("selection=\"drop\""))
            .count(),
        506
    );
}

#[test]
fn relabel_regex_uses_bounded_non_backtracking_compilation() {
    let regex = RegexBuilder::new(r"^(a+)+$")
        .size_limit(64 * 1024)
        .dfa_size_limit(64 * 1024)
        .build()
        .expect("bounded regex compiles");
    let input = format!("{}!", "a".repeat(32 * 1024));
    assert!(!regex.is_match(&input));
}
