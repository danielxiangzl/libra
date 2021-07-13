// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, Result};
use diem_state_view::StateView;
use diem_types::{
    access_path::AccessPath, transaction::TransactionOutput, vm_status::StatusCode,
    write_set::WriteOp,
};
use move_binary_format::errors::*;
use move_core_types::{
    account_address::AccountAddress,
    language_storage::{ModuleId, StructTag},
};
use move_vm_runtime::data_cache::MoveStorage;
use mvhashmap::{MVHashMap, DynMVHashMap};
use std::{cmp, borrow::Cow, collections::HashSet, convert::AsRef, thread, time::Duration};

pub struct VersionedDataCache(MVHashMap<AccessPath, Vec<u8>>, DynMVHashMap<AccessPath, Vec<u8>>);

pub struct VersionedStateView<'view> {
    version: usize,
    base_view: &'view dyn StateView,
    placeholder: &'view VersionedDataCache,
}

const ONE_MILLISEC: Duration = Duration::from_millis(10);

impl VersionedDataCache {
    pub fn new(write_sequence: Vec<(AccessPath, usize)>) -> (usize, Self) {
        let (max_dependency_length, mv_hashmap) = MVHashMap::new_from_parallel(write_sequence);
        let dyn_mvhashmap = DynMVHashMap::new();
        (max_dependency_length, VersionedDataCache(mv_hashmap, dyn_mvhashmap))
    }

    pub fn set_skip_all(&self, version: usize, estimated_writes: impl Iterator<Item = AccessPath>) {
        // Put skip in all entires.
        for w in estimated_writes {
            // It should be safe to unwrap here since the MVMap was construted using
            // this estimated writes. If not it is a bug.
            self.as_ref().skip(&w, version).unwrap();
        }
    }

    pub fn set_dirty_to_static(&self, version: usize, write_set: HashSet<AccessPath>) {
        for w in write_set {
            // It should be safe to unwrap here since the MVMap was construted using
            // this estimated writes. If not it is a bug.
            self.0.set_dirty(&w, version).unwrap();
        }
    }

    pub fn set_dirty_to_dynamic(&self, version: usize, write_set: HashSet<AccessPath>) {
        for w in write_set {
            // It should be safe to unwrap here since the MVMap was dynamic
            self.1.set_dirty(&w, version).unwrap();
        }
    }

    // Apply the writes to static mvhashmap
    pub fn apply_output_to_static(
        &self,
        output: &TransactionOutput,
        version: usize,
        estimated_writes: impl Iterator<Item = AccessPath>,
    ) -> Result<(), ()> {
        if !output.status().is_discarded() {
            // First make sure all outputs have entries in the MVMap
            let estimated_writes_hs: HashSet<_> = estimated_writes.collect();
            for (k, _) in output.write_set() {
                if !estimated_writes_hs.contains(k) {
                    println!("Missing entry: {:?}", k);

                    // Put skip in all entires.
                    self.set_skip_all(version, estimated_writes_hs.into_iter());
                    return Err(());
                }
            }

            // We are sure all entries are present -- now update them all
            for (k, v) in output.write_set() {
                let val = match v {
                    WriteOp::Deletion => None,
                    WriteOp::Value(data) => Some(data.clone()),
                };

                // Safe because we checked that the entry exists
                self.as_ref().write(k, version, val).unwrap();
            }

            // If any entries are not updated, write a 'skip' flag into them
            for w in estimated_writes_hs {
                // It should be safe to unwrap here since the MVMap was construted using
                // this estimated writes. If not it is a bug.
                self.as_ref()
                    .skip_if_not_set(&w, version)
                    .expect("Entry must exist.");
            }
        } else {
            self.set_skip_all(version, estimated_writes);
        }
        Ok(())
    }

    // Apply the writes to dynamic mvhashmap (only for testing)
    pub fn apply_output_to_dynamic(
        &self,
        output: &TransactionOutput,
        version: usize,
        _estimated_writes: impl Iterator<Item = AccessPath>,
    ) -> Result<(), ()> {
        if !output.status().is_discarded() {
            for (k, v) in output.write_set() {
                let val = match v {
                    WriteOp::Deletion => None,
                    WriteOp::Value(data) => Some(data.clone()),
                };

                // Safe because the mvhashmap is dynamic
                self.1.write(k, version, val).unwrap();
            }
        }
        Ok(())
    }

    // Apply the writes to both static and dynamic mvhashmap
    pub fn apply_output_to_both(
        &self,
        output: &TransactionOutput,
        version: usize,
        estimated_writes: impl Iterator<Item = AccessPath> + Copy,
    ) -> Result<(), ()> {
        // First get all non-estimated writes
        let estimated_writes_hs: HashSet<AccessPath> = estimated_writes.collect();
        let mut non_estimated_writes_hs: HashSet<AccessPath> = HashSet::new();
        for (k, _) in output.write_set() {
            if !estimated_writes_hs.contains(k) {
                non_estimated_writes_hs.insert(k.clone());
            }
        }
        if !output.status().is_discarded() {
            for (k, v) in output.write_set() {
                let val = match v {
                    WriteOp::Deletion => None,
                    WriteOp::Value(data) => Some(data.clone()),
                };
                // Write estimated writes to static mvhashmap, and write non-estimated ones to dynamic mvhashmap
                if estimated_writes_hs.contains(k) {
                    self.0.write(k, version, val).unwrap();
                } else {
                    self.1.write(k, version, val).unwrap();
                }
            }

            // If any entries are not updated, write a 'skip' flag into them
            for w in estimated_writes_hs {
                // It should be safe to unwrap here since the MVMap was construted using
                // this estimated writes. If not it is a bug.
                self.as_ref()
                    .skip_if_not_set(&w, version)
                    .expect("Entry must exist.");
            }
        } else {
            self.set_skip_all(version, estimated_writes);
        }
        Ok(())
    }

    pub fn read_from_both(&self, access_path: &AccessPath, version: usize) -> Result<(Option<Vec<u8>>, Option<usize>), Option<usize>> {
        // one access_path may appear in both mvhashmaps, since it maybe estimated for one txn and non-estimated for another txn
        let static_has_key = self.0.contains_key(access_path);
        let dyn_has_key = self.1.contains_key(access_path);

        // If both mvhashmaps do not contain AP, return Err(None)
        if !static_has_key && !dyn_has_key {
            return Err(None);
        }

        // If both mvhashmaps contain AP, return the higher version
        if static_has_key && dyn_has_key {
            // reads may return Ok((Option<V>, Some<Version>)), Err(Some<Version>) or Err(None)
            let read_from_static = self.0.read(access_path, version);
            let read_from_dynamic = self.1.read(access_path, version);
            // Should return the dependency or data of the higher version
            let version1 = match read_from_static {
                Ok((_, version)) => version,
                Err(version) => version,
            };
            let version2 = match read_from_dynamic {
                Ok((_, version)) => version,
                Err(version) => version,
            };
            if version1.is_some() && version2.is_some() {
                if version1.unwrap() > version2.unwrap() {
                    return read_from_static;
                } else {
                    return read_from_dynamic;
                }
            }
            if version1.is_none() {
                return read_from_dynamic;
            } else {
                return read_from_static;
            }
        } else if static_has_key {
            // reads may return Ok((Option<V>, Some<Version>)), Err(Some<Version>) or Err(None)
            return self.0.read(access_path, version);
        } else {
            return self.1.read(access_path, version);
        }
    }

    pub fn read_from_static(&self, access_path: &AccessPath, version: usize) -> Result<(Option<Vec<u8>>, Option<usize>), Option<usize>> {
        return self.0.read(access_path, version);
    }

    pub fn read_from_dynamic(&self, access_path: &AccessPath, version: usize) -> Result<(Option<Vec<u8>>, Option<usize>), Option<usize>> {
        return self.1.read(access_path, version);
    }
}

impl AsRef<MVHashMap<AccessPath, Vec<u8>>> for VersionedDataCache {
    fn as_ref(&self) -> &MVHashMap<AccessPath, Vec<u8>> {
        &self.0
    }
}

impl<'view> VersionedStateView<'view> {
    pub fn new(
        version: usize,
        base_view: &'view dyn StateView,
        placeholder: &'view VersionedDataCache,
    ) -> VersionedStateView<'view> {
        VersionedStateView {
            version,
            base_view,
            placeholder,
        }
    }

    pub fn will_read_block(&self, access_path: &AccessPath) -> bool {
        let read = self.placeholder.as_ref().read(access_path, self.version);
        if let Err(Some(_)) = read {
            return true;
        }
        return false;
    }

    // Return Some(version) when reading access_path is blocked by transaction of id=version, otherwise return None
    // Only read from static mvhashmap
    pub fn will_read_from_static_block_return_version(&self, access_path: &AccessPath) -> Option<usize> {
        let read = self.placeholder.read_from_static(access_path, self.version);
        if let Err(Some(version)) = read {
            return Some(version);
        }
        return None;
    }

    // Return Some(version) when reading access_path is blocked by transaction of id=version, otherwise return None
    // Only read from dynamic mvhashmap (only for testing)
    pub fn will_read_from_dynamic_block_return_version(&self, access_path: &AccessPath) -> Option<usize> {
        // First read from the static mvhashmap to get the dependency
        // Should either return Err(Some(version)) or Err(None)
        let read_from_static = self.placeholder.read_from_static(access_path, self.version);
        let dep = match read_from_static {
            Err(None) => return None,
            Err(Some(version)) => version,
            _ => {
                println!("ERROR!");
                return None;
            },
        };
        // Then read from the dynamic mvhashmap
        // If read the right version, return None, otherwise return dependency
        let read_from_dynamic = self.placeholder.read_from_dynamic(access_path, self.version);
        if let Ok((_, Some(version))) = read_from_dynamic {
            if dep == version {
                return None;
            }
        }
        return Some(dep);
    }

    // Return Some(version) when reading access_path is blocked by transaction of id=version, otherwise return None
    // Read from both static and dynamic mvhashmap
    pub fn will_read_from_both_block_return_version(&self, access_path: &AccessPath) -> Option<usize> {
        let read = self.placeholder.read_from_both(access_path, self.version);
        if let Err(Some(version)) = read {
            return Some(version);
        }
        return None;
    }

    fn get_bytes_ref(&self, access_path: &AccessPath) -> PartialVMResult<Option<Cow<[u8]>>> {
        let mut loop_iterations = 0;
        loop {
            let read = self.placeholder.read_from_dynamic(access_path, self.version);

            // Go to the Database
            if let Err(None) = read {
                return self
                    .base_view
                    .get(access_path)
                    .map(|opt| opt.map(Cow::from))
                    .map_err(|_| PartialVMError::new(StatusCode::STORAGE_ERROR));
            }

            // Read is a success
            if let Ok((data, _)) = read {
                return Ok(data.map(Cow::from));
            }

            loop_iterations += 1;
            if loop_iterations < 500 {
                ::std::hint::spin_loop();
            } else {
                thread::sleep(ONE_MILLISEC);
            }
        }
    }

    fn get_bytes(&self, access_path: &AccessPath) -> PartialVMResult<Option<Vec<u8>>> {
        self.get_bytes_ref(access_path)
            .map(|opt| opt.map(Cow::into_owned))
    }
}

impl<'view> MoveStorage for VersionedStateView<'view> {
    fn get_module(&self, module_id: &ModuleId) -> VMResult<Option<Vec<u8>>> {
        // REVIEW: cache this?
        let ap = AccessPath::from(module_id);
        self.get_bytes(&ap)
            .map_err(|e| e.finish(Location::Undefined))
    }

    fn get_resource(
        &self,
        address: &AccountAddress,
        struct_tag: &StructTag,
    ) -> PartialVMResult<Option<Cow<[u8]>>> {
        let ap = AccessPath::new(
            *address,
            AccessPath::resource_access_vec(struct_tag.clone()),
        );
        self.get_bytes_ref(&ap)
    }
}

impl<'view> StateView for VersionedStateView<'view> {
    fn get(&self, access_path: &AccessPath) -> Result<Option<Vec<u8>>> {
        self.get_bytes(access_path)
            .map_err(|_| anyhow!("Failed to get data from VersionedStateView"))
    }

    fn is_genesis(&self) -> bool {
        self.base_view.is_genesis()
    }
}

//
// pub struct SingleThreadReadCache<'view> {
//     base_view : &'view dyn StateView,
//     cache : RefCell<HashMap<AccessPath, Option<Vec<u8>>>>,
// }
//
// impl<'view> SingleThreadReadCache<'view> {
//     pub fn new(base_view: &'view StateView) -> SingleThreadReadCache<'view> {
//         SingleThreadReadCache {
//             base_view,
//             cache : RefCell::new(HashMap::new()),
//         }
//     }
// }
//
// impl<'view> StateView for SingleThreadReadCache<'view> {
//     // Get some data either through the cache or the `StateView` on a cache miss.
//     fn get(&self, access_path: &AccessPath) -> anyhow::Result<Option<Vec<u8>>> {
//         if self.cache.borrow().contains_key(access_path) {
//             Ok(self.cache.borrow().get(access_path).unwrap().clone())
//         }
//         else {
//             let val = self.base_view.get(access_path)?;
//             self.cache.borrow_mut().insert(access_path.clone(), val.clone());
//             Ok(val)
//         }
//     }
//
//     fn multi_get(&self, _access_paths: &[AccessPath]) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
//         unimplemented!()
//     }
//
//     fn is_genesis(&self) -> bool {
//         self.base_view.is_genesis()
//     }
// }
