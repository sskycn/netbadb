mod phase35_tests {
    use super::*;

    const MODES: [&str; 3] = [
        "physical_index_admission",
        "physical_columnar_snapshot_admission",
        "physical_columnar_incremental_admission",
    ];
    const DIMENSIONS: [&str; 6] = [
        "source_work_units",
        "source_read_bytes",
        "prerequisite_work_units",
        "prerequisite_read_bytes",
        "prerequisite_write_bytes",
        "output_write_bytes",
    ];

    fn policy(maximum: u64) -> serde_json::Value {
        let mut value = json!({"mode":"component_limits"});
        for field in DIMENSIONS {
            value[field] = json!({"kind":"unconstrained"});
        }
        value["source_read_bytes"] = json!({"kind":"at_most", "maximum":maximum});
        value
    }

    #[test]
    fn manifest_v11_modes_are_required_strict_independent_and_core_validated() {
        let directory = test_directory("phase35-modes");
        std::fs::create_dir_all(directory.join("placements")).unwrap();
        create_heap(&directory.join("users.ndb"));
        let path = directory.join("server.json");
        let mut value: serde_json::Value =
            serde_json::from_str(&manifest_json(None, "users.ndb", "UserId")).unwrap();
        let parse = |value: &serde_json::Value| {
            std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
            ServerConfig::from_manifest_path(&path)
        };
        assert!(parse(&value).unwrap().operator_config().is_none());
        for version in (1..=10).chain([12, u32::MAX]) {
            let mut old = value.clone();
            old["version"] = json!(version);
            assert!(
                matches!(parse(&old), Err(ManifestError::UnsupportedVersion(actual)) if actual == version)
            );
        }
        value["physical_design"] = physical_design_json();
        value["physical_design"]["columnar_apply"] =
            json!({"root":"placements", "allow_snapshot":true, "allow_incremental":true});
        value["operator"] = json!({"unix_socket":"operator.sock", "io_timeout_ms":5000,
            "allow_physical_index_apply":true, "allow_physical_columnar_apply":true,
            "allow_physical_design_receipt_read":false});
        for field in MODES {
            value["operator"][field] = json!({"mode":"unadmitted"});
        }
        let migrated = parse(&value).unwrap();
        assert_eq!(
            migrated.operator_config().unwrap().admissions,
            ServerOperatorMutationAdmissions::UNADMITTED
        );
        for mask in 0..8 {
            for maximum in [0, u64::MAX] {
                let mut current = value.clone();
                for (index, field) in MODES.into_iter().enumerate() {
                    if mask & (1 << index) != 0 {
                        current["operator"][field] = policy(maximum);
                    }
                }
                let config = parse(&current).unwrap();
                let modes = config.operator_config().unwrap().admissions;
                for (index, mode) in [modes.index, modes.snapshot, modes.incremental]
                    .into_iter()
                    .enumerate()
                {
                    if mask & (1 << index) == 0 {
                        assert_eq!(
                            mode,
                            ServerOperatorPhysicalDesignMutationAdmission::Unadmitted
                        );
                    } else {
                        let ServerOperatorPhysicalDesignMutationAdmission::ComponentLimits(policy) =
                            mode
                        else {
                            panic!("component limits")
                        };
                        assert_eq!(
                            policy.limits().source_read_bytes,
                            PhysicalDesignMutationAdmissionConstraint::AtMost(maximum)
                        );
                    }
                }
            }
        }
        for field in MODES {
            let mut missing = value.clone();
            missing["operator"].as_object_mut().unwrap().remove(field);
            assert!(matches!(parse(&missing), Err(ManifestError::Json(_))));
            for malformed in [
                json!(null),
                json!({"mode":"other"}),
                json!({"mode":"unadmitted","policy":{}}),
            ] {
                let mut invalid = value.clone();
                invalid["operator"][field] = malformed;
                assert!(matches!(parse(&invalid), Err(ManifestError::Json(_))));
            }
            for dimension in DIMENSIONS {
                let mut invalid = value.clone();
                invalid["operator"][field] = policy(1);
                invalid["operator"][field]
                    .as_object_mut()
                    .unwrap()
                    .remove(dimension);
                assert!(matches!(parse(&invalid), Err(ManifestError::Json(_))));
                for malformed in [
                    json!(null),
                    json!(1000),
                    json!({"kind":"unknown"}),
                    json!({"kind":"at_most"}),
                    json!({"kind":"at_most","maximum":-1}),
                    json!({"kind":"at_most","maximum":1,"extra":1}),
                    json!({"kind":"unconstrained","maximum":1}),
                ] {
                    invalid["operator"][field][dimension] = malformed;
                    assert!(matches!(parse(&invalid), Err(ManifestError::Json(_))));
                }
            }
            let mut invalid = value.clone();
            invalid["operator"][field] = policy(1);
            invalid["operator"][field]["extra"] = json!(true);
            assert!(matches!(parse(&invalid), Err(ManifestError::Json(_))));
            invalid["operator"][field] = policy(1);
            invalid["operator"][field]["source_read_bytes"] = json!({"kind":"unconstrained"});
            let error = parse(&invalid).unwrap_err();
            assert!(matches!(
                error,
                ManifestError::PhysicalDesignMutationAdmissionPolicy {
                    source: PhysicalDesignMutationAdmissionPolicyError::NoConstrainedDimension,
                    ..
                }
            ));
            assert!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<PhysicalDesignMutationAdmissionPolicyError>()
                    .is_some()
            );
            invalid["operator"][field] = policy(1);
            let permission = if field == MODES[0] {
                "allow_physical_index_apply"
            } else {
                "allow_physical_columnar_apply"
            };
            invalid["operator"][permission] = json!(false);
            assert!(
                matches!(parse(&invalid), Err(ManifestError::OperatorAdmissionRequiresEnabledMode { field: actual }) if actual == field)
            );
            if field != MODES[0] {
                invalid["operator"][permission] = json!(true);
                let allowed = if field == MODES[1] {
                    "allow_snapshot"
                } else {
                    "allow_incremental"
                };
                invalid["physical_design"]["columnar_apply"][allowed] = json!(false);
                assert!(
                    matches!(parse(&invalid), Err(ManifestError::OperatorAdmissionRequiresEnabledMode { field: actual }) if actual == field)
                );
            }
        }
        assert!(!directory.join("operator.sock").exists());
        assert!(
            std::fs::read_dir(directory.join("placements"))
                .unwrap()
                .next()
                .is_none()
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn manifest_v11_document_example_parses_without_evaluating_current_bounds() {
        let directory = test_directory("phase35-document");
        std::fs::create_dir_all(directory.join("data/columnar")).unwrap();
        std::fs::create_dir_all(directory.join("run")).unwrap();
        let table = TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64))
                    .primary_key(true),
            ],
        );
        Database::create(directory.join("data/users.ndb"), table)
            .unwrap()
            .close()
            .unwrap();
        let example = include_str!("../../../docs/server-manifest-v11.md")
            .split("```json\n")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap();
        let manifest = directory.join("server.json");
        std::fs::write(&manifest, example).unwrap();
        assert_eq!(
            ServerConfig::from_manifest_path(&manifest)
                .unwrap()
                .operator_config()
                .unwrap()
                .admissions,
            ServerOperatorMutationAdmissions::UNADMITTED
        );
        assert!(
            !directory
                .join("data/physical-design-receipts.nbmr")
                .exists()
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
}
