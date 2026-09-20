mod phase35_tests {
    use super::*;
    use netbadb_core::{
        PhysicalDesignMutationAdmissionConstraint::{AtMost, Unconstrained},
        PhysicalDesignMutationAdmissionLimits,
    };

    #[test]
    fn v7_budget_injection_is_rejected_and_historical_headers_are_rejected() {
        for operation in [
            serde_json::json!({"type":"apply_physical_index","expected_runtime_token":"11".repeat(16),"expected_evidence_epoch":7,"table_id":1,"column_id":3,"index_name":"idx"}),
            serde_json::json!({"type":"apply_physical_columnar","expected_runtime_token":"11".repeat(16),"expected_evidence_epoch":7,"table_id":1,"columns":[2,3],"mode":"snapshot","placement_key":"projection"}),
        ] {
            let request = serde_json::json!({"request_id":1,"operation":operation});
            let bytes = encode_frame(&request).unwrap();
            assert_eq!(&bytes[..8], b"NBOP\0\x07\0\0");
            assert!(read_frame::<OperatorRequestV7>(&mut bytes.as_slice()).is_ok());
            for version in [5_u16, 6] {
                let mut old = bytes.clone();
                old[4..6].copy_from_slice(&version.to_be_bytes());
                assert!(matches!(
                    read_frame::<OperatorRequestV7>(&mut old.as_slice()),
                    Err(OperatorProtocolError::UnsupportedVersion(rejected))
                        if rejected == version
                ));
            }
            for field in [
                "budget",
                "limits",
                "admission",
                "max_source_read_bytes",
                "work_units",
                "read_bytes",
                "write_bytes",
                "inspection",
                "expected_bound",
            ] {
                let mut injected = request.clone();
                injected["operation"][field] = serde_json::json!({});
                assert!(
                    serde_json::from_value::<OperatorRequestV7>(injected).is_err(),
                    "{field}"
                );
            }
        }
    }

    #[test]
    fn v7_status_presents_exact_independent_policies_and_errors_are_private() {
        let limits = PhysicalDesignMutationAdmissionLimits {
            source_work_units: AtMost(0),
            source_read_bytes: AtMost(u64::MAX),
            prerequisite_work_units: Unconstrained,
            prerequisite_read_bytes: AtMost(17),
            prerequisite_write_bytes: AtMost(29),
            output_write_bytes: Unconstrained,
        };
        let admitted = ServerOperatorPhysicalDesignMutationAdmission::ComponentLimits(
            PhysicalDesignMutationAdmissionPolicy::new(limits).unwrap(),
        );
        let dto = OperatorPhysicalDesignMutationAdmissionModeV7::from(admitted);
        let json = serde_json::to_value(dto).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"mode":"component_limits", "policy":{
                "source_work_units":{"kind":"at_most","maximum":0},
                "source_read_bytes":{"kind":"at_most","maximum":u64::MAX},
                "prerequisite_work_units":{"kind":"unconstrained"},
                "prerequisite_read_bytes":{"kind":"at_most","maximum":17},
                "prerequisite_write_bytes":{"kind":"at_most","maximum":29},
                "output_write_bytes":{"kind":"unconstrained"}
            }})
        );
        assert_eq!(
            serde_json::from_value::<OperatorPhysicalDesignMutationAdmissionModeV7>(json).unwrap(),
            dto
        );
        for (dimension, tag) in [
            (
                PhysicalDesignMutationAdmissionDimension::SourceWorkUnits,
                "source_work_units",
            ),
            (
                PhysicalDesignMutationAdmissionDimension::SourceReadBytes,
                "source_read_bytes",
            ),
            (
                PhysicalDesignMutationAdmissionDimension::PrerequisiteWorkUnits,
                "prerequisite_work_units",
            ),
            (
                PhysicalDesignMutationAdmissionDimension::PrerequisiteReadBytes,
                "prerequisite_read_bytes",
            ),
            (
                PhysicalDesignMutationAdmissionDimension::PrerequisiteWriteBytes,
                "prerequisite_write_bytes",
            ),
            (
                PhysicalDesignMutationAdmissionDimension::OutputWriteBytes,
                "output_write_bytes",
            ),
        ] {
            for receipt in [
                None,
                Some(OperatorPhysicalDesignMutationReceiptRefV7 {
                    journal_incarnation: "11".repeat(16),
                    receipt_id: 7,
                }),
            ] {
                for error in [
                    PhysicalDesignMutationAdmissionError::RequiredBoundNotProven { dimension },
                    PhysicalDesignMutationAdmissionError::LimitExceeded {
                        dimension,
                        conservative_bound: 19,
                        maximum: 18,
                    },
                ] {
                    let remote = admission_remote_error(error, receipt.clone());
                    assert_eq!(
                        remote.code,
                        OperatorErrorCodeV7::PhysicalDesignMutationAdmissionRejected
                    );
                    assert_eq!(remote.receipt, receipt);
                    let json = serde_json::to_value(&remote).unwrap();
                    assert_eq!(json["admission"]["dimension"], tag);
                    assert_eq!(
                        serde_json::from_value::<OperatorRemoteErrorV7>(json).unwrap(),
                        remote
                    );
                }
            }
        }
        let non_admission = physical_design_remote_error(
            ServerPhysicalDesignControlError::PhysicalDesignRuntimeChanged,
        );
        assert_eq!(
            serde_json::to_value(non_admission).unwrap()["admission"],
            serde_json::Value::Null
        );
    }

    #[test]
    fn v7_recovery_required_admission_json_is_exact_with_nullable_receipt() {
        fn kind(
            rejection: OperatorPhysicalDesignMutationAdmissionRejectionV7,
        ) -> &'static str {
            match rejection {
                OperatorPhysicalDesignMutationAdmissionRejectionV7::RequiredBoundNotProven {
                    ..
                } => "required_bound_not_proven",
                OperatorPhysicalDesignMutationAdmissionRejectionV7::LimitExceeded { .. } => {
                    "limit_exceeded"
                }
                OperatorPhysicalDesignMutationAdmissionRejectionV7::InspectionFailed {} => {
                    "inspection_failed"
                }
                OperatorPhysicalDesignMutationAdmissionRejectionV7::RecoveryRequired {} => {
                    "recovery_required"
                }
            }
        }

        assert_eq!(
            kind(
                serde_json::from_str::<OperatorPhysicalDesignMutationAdmissionRejectionV7>(
                    r#"{"kind":"recovery_required"}"#,
                )
                .unwrap(),
            ),
            "recovery_required"
        );

        for receipt in [
            None,
            Some(OperatorPhysicalDesignMutationReceiptRefV7 {
                journal_incarnation: "00112233445566778899aabbccddeeff".to_owned(),
                receipt_id: 41,
            }),
        ] {
            let remote = OperatorRemoteErrorV7 {
                admission: Some(
                    OperatorPhysicalDesignMutationAdmissionRejectionV7::RecoveryRequired {},
                ),
                code: OperatorErrorCodeV7::PhysicalDesignMutationAdmissionRejected,
                message:
                    "current mutation-work inspection requires restart/reopen before retry"
                        .to_owned(),
                receipt: receipt.clone(),
            };
            let expected = serde_json::json!({
                "admission": {"kind": "recovery_required"},
                "code": "physical_design_mutation_admission_rejected",
                "message": "current mutation-work inspection requires restart/reopen before retry",
                "receipt": receipt.as_ref().map(|receipt| serde_json::json!({
                    "journal_incarnation": receipt.journal_incarnation,
                    "receipt_id": receipt.receipt_id,
                })),
            });
            assert_eq!(serde_json::to_value(&remote).unwrap(), expected);
            assert_eq!(
                serde_json::from_value::<OperatorRemoteErrorV7>(expected).unwrap(),
                remote
            );
        }
    }
}
