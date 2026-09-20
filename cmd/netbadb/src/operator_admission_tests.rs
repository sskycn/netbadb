mod admission_tests {
    use super::*;
    use OperatorPhysicalDesignMutationAdmissionConstraintV7::{AtMost, Unconstrained};
    use OperatorPhysicalDesignMutationAdmissionDimensionV7 as Dimension;
    use OperatorPhysicalDesignMutationAdmissionModeV7::{ComponentLimits, Unadmitted};
    use OperatorPhysicalDesignMutationAdmissionRejectionV7 as Rejection;
    use netbadb_server::OperatorPhysicalDesignMutationAdmissionPolicyV7 as Policy;

    #[test]
    fn cli_renders_explicit_component_policies_and_definitive_rejections() {
        let policy = Policy {
            source_work_units: AtMost { maximum: 0 },
            source_read_bytes: AtMost { maximum: 10000 },
            prerequisite_work_units: Unconstrained {},
            prerequisite_read_bytes: Unconstrained {},
            prerequisite_write_bytes: AtMost { maximum: u64::MAX },
            output_write_bytes: Unconstrained {},
        };
        for label in ["Index", "Columnar Snapshot", "Columnar Incremental"] {
            let mut output = String::new();
            render_admission_mode(&mut output, label, Unadmitted {});
            assert_eq!(output, format!("{label} admission: unadmitted\n"));
            output.clear();
            render_admission_mode(&mut output, label, ComponentLimits { policy });
            assert_eq!(output.lines().count(), 7);
            assert!(output.contains("source work units: at most 0"));
            assert!(output.contains("source read bytes: at most 10000"));
            assert!(output.contains("output write bytes: unconstrained"));
            assert!(!output.contains("total") && !output.contains("safe"));
        }
        for admission in [
            Rejection::RequiredBoundNotProven {
                dimension: Dimension::SourceWorkUnits,
            },
            Rejection::LimitExceeded {
                dimension: Dimension::SourceReadBytes,
                conservative_bound: 12345,
                maximum: 10000,
            },
            Rejection::InspectionFailed {},
            Rejection::RecoveryRequired {},
        ] {
            for receipt in [
                None,
                Some(OperatorPhysicalDesignMutationReceiptRefV7 {
                    journal_incarnation: "11".repeat(16),
                    receipt_id: 7,
                }),
            ] {
                let error = classify_operator_apply_error(OperatorClientError::Remote(
                    OperatorRemoteErrorV7 {
                        code: OperatorErrorCodeV7::PhysicalDesignMutationAdmissionRejected,
                        message: "private server message must not be used".into(),
                        receipt: receipt.clone(),
                        admission: Some(admission.clone()),
                    },
                ));
                assert!(matches!(error, OperationalError::Operator(_)));
                let output = error.to_string();
                assert!(output.starts_with("admission rejected:"));
                assert_eq!(output.contains("receipt"), receipt.is_some());
                assert!(!output.contains("private"));
                match admission {
                    Rejection::RequiredBoundNotProven { .. } => assert!(output.contains("source_work_units has no proven current conservative bound")),
                    Rejection::LimitExceeded { .. } => assert!(output.contains("source_read_bytes conservative bound 12345 exceeds configured maximum 10000")),
                    Rejection::InspectionFailed {} => assert!(output.contains("current mutation-work inspection failed before mutation")),
                    Rejection::RecoveryRequired {} => assert!(output.contains("current mutation-work inspection requires restart/reopen before retry")),
                }
            }
        }
    }

    #[test]
    fn cli_apply_rejects_every_budget_flag() {
        for command in ["apply-index", "apply-columnar"] {
            for flag in [
                "--budget",
                "--max-work",
                "--max-read-bytes",
                "--max-write-bytes",
                "--max-source-work-units",
                "--max-source-read-bytes",
            ] {
                assert!(matches!(
                    parse_args(args(&["operator", "physical-design", command, flag, "1"])),
                    Err(UsageError::UnknownArgument(_))
                ));
            }
        }
    }
}
