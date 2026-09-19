mod phase35_tests {
    use super::*;
    use netbadb_core::{
        PhysicalDesignMutationAdmissionConstraint::{AtMost, Unconstrained},
        PhysicalDesignMutationAdmissionLimits,
    };

    #[test]
    fn v6_budget_injection_is_rejected_and_v5_header_is_rejected() {
        for operation in [
            serde_json::json!({"type":"apply_physical_index","expected_runtime_token":"11".repeat(16),"expected_evidence_epoch":7,"table_id":1,"column_id":3,"index_name":"idx"}),
            serde_json::json!({"type":"apply_physical_columnar","expected_runtime_token":"11".repeat(16),"expected_evidence_epoch":7,"table_id":1,"columns":[2,3],"mode":"snapshot","placement_key":"projection"}),
        ] {
            let request = serde_json::json!({"request_id":1,"operation":operation});
            let bytes = encode_frame(&request).unwrap();
            assert_eq!(&bytes[..8], b"NBOP\0\x06\0\0");
            assert!(read_frame::<OperatorRequestV6>(&mut bytes.as_slice()).is_ok());
            let mut old = bytes.clone();
            old[4..6].copy_from_slice(&5_u16.to_be_bytes());
            assert!(matches!(
                read_frame::<OperatorRequestV6>(&mut old.as_slice()),
                Err(OperatorProtocolError::UnsupportedVersion(5))
            ));
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
                    serde_json::from_value::<OperatorRequestV6>(injected).is_err(),
                    "{field}"
                );
            }
        }
    }

    #[test]
    fn v6_status_presents_exact_independent_policies_and_errors_are_private() {
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
        let dto = OperatorPhysicalDesignMutationAdmissionModeV6::from(admitted);
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
            serde_json::from_value::<OperatorPhysicalDesignMutationAdmissionModeV6>(json).unwrap(),
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
                Some(OperatorPhysicalDesignMutationReceiptRefV6 {
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
                        OperatorErrorCodeV6::PhysicalDesignMutationAdmissionRejected
                    );
                    assert_eq!(remote.receipt, receipt);
                    let json = serde_json::to_value(&remote).unwrap();
                    assert_eq!(json["admission"]["dimension"], tag);
                    assert_eq!(
                        serde_json::from_value::<OperatorRemoteErrorV6>(json).unwrap(),
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
}
