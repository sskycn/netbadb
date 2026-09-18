use std::sync::{Arc, Mutex, MutexGuard};

/// One outstanding synchronous control per handle family, including all clones.
/// Admission precedes reply allocation and proposal copying and lasts through
/// the reply. The listener and Database worker never acquire this gate, so
/// shutdown can drop their receivers and wake the admitted caller.
#[derive(Clone, Default)]
pub(crate) struct ControlAdmission(Arc<Mutex<()>>);

impl ControlAdmission {
    pub(crate) fn enter(&self) -> MutexGuard<'_, ()> {
        // There is no protected mutable state to repair after a caller panics.
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(test)]
    pub(crate) fn is_occupied(&self) -> bool {
        matches!(self.0.try_lock(), Err(std::sync::TryLockError::WouldBlock))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn cloned_control_admission_serializes_callers_and_recovers_after_panic() {
        let admission = ControlAdmission::default();
        let active = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..16 {
                let clone = admission.clone();
                let active = &active;
                scope.spawn(move || {
                    for _ in 0..100 {
                        let _permit = clone.enter();
                        assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
                        assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
                    }
                });
            }
        });
        let clone = admission.clone();
        assert!(
            std::thread::spawn(move || {
                let _permit = clone.enter();
                panic!("caller failure");
            })
            .join()
            .is_err()
        );
        let _permit = admission.enter();
        assert!(admission.clone().is_occupied());
    }
}
