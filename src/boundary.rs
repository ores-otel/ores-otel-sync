#![forbid(unsafe_code)]

use crate::error::SyncError;
use crate::sqlite::SqliteStore;

#[derive(Clone, Debug)]
pub struct SyncBoundary {
    pub store: SqliteStore,
    pub enabled: bool,
    /// Admission-only check for a batch, before a caller persists or transmits it.
    /// Enforces each record's bound, an aggregate byte budget and a record-count
    /// limit without allocating or partially admitting a batch.
    pub fn admit_batch(&self, batch: &[&[u8]]) -> Result<usize, SyncError> {
        const MAX_BATCH_RECORDS: usize = 256;
        const MAX_BATCH_BYTES: usize = 1024 * 1024;
        if !self.enabled {
            return Err(SyncError::Disabled);
        }
        if batch.len() > MAX_BATCH_RECORDS {
            return Err(SyncError::TooLarge);
        }
        let mut total = 0usize;
        for bytes in batch {
            self.envelope(bytes)?;
            total = total.checked_add(bytes.len()).ok_or(SyncError::TooLarge)?;
            if total > MAX_BATCH_BYTES {
                return Err(SyncError::TooLarge);
            }
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boundary(enabled: bool) -> SyncBoundary {
        SyncBoundary { store: SqliteStore::default(), enabled }
    }

    #[test]
    fn admits_bounded_batches_without_modifying_store() {
        let first = [7u8; 3];
        let second = [9u8; 5];
        assert_eq!(boundary(true).admit_batch(&[&first, &second]), Ok(8));
        assert_eq!(boundary(true).admit_batch(&[]), Ok(0));
    }

    #[test]
    fn rejects_disabled_boundary_even_for_empty_batch() {
        assert!(matches!(boundary(false).admit_batch(&[]), Err(SyncError::Disabled)));
    }

    #[test]
    fn rejects_record_count_overflow_and_individual_oversize() {
        let byte = [1u8; 1];
        let entries = vec![byte.as_slice(); 257];
        assert!(matches!(boundary(true).admit_batch(&entries), Err(SyncError::TooLarge)));
        let oversized = vec![2u8; 64 * 1024 + 1];
        assert!(matches!(boundary(true).admit_batch(&[&oversized]), Err(SyncError::TooLarge)));
    }

    #[test]
    fn rejects_aggregate_bytes_above_one_mib() {
        let record = vec![0u8; 64 * 1024];
        let sixteen = vec![record.as_slice(); 16];
        let seventeen = vec![record.as_slice(); 17];
        assert_eq!(boundary(true).admit_batch(&sixteen), Ok(1024 * 1024));
        assert!(matches!(boundary(true).admit_batch(&seventeen), Err(SyncError::TooLarge)));
    }
}
impl SyncBoundary {
    pub fn envelope(&self, bytes: &[u8]) -> Result<usize, SyncError> {
        if !self.enabled {
            return Err(SyncError::Disabled);
        }
        if bytes.len() > 64 * 1024 {
            return Err(SyncError::TooLarge);
        }
        Ok(bytes.len())
    }
}

