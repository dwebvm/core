#[macro_use]
extern crate serde_derive;

use hypermachines_sys::{host_request, read_arguments, rpc_response};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_bytes::ByteBuf;
use std::collections::HashMap;

// ID Types
pub type CoreId = u32;
pub type DatabaseId = u32;

// Hypercore Types
pub type CoreKey = Vec<u8>;
pub type Seq = u64;
pub type ByteLength = u64;

pub type Block = ByteBuf;

// Hyperbee Types
pub type DatabaseKey = String;
pub type DatabaseValue = Block;

#[derive(Serialize, Deserialize)]
pub struct DatabaseRecord {
    pub key: DatabaseKey,
    #[serde(with = "serde_bytes")]
    pub value: Option<DatabaseValue>,
}

// Data Structs

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoreSpec {
    #[serde(with = "serde_bytes")]
    key: CoreKey,
    length: Option<Seq>,
    writable: Option<bool>,
    is_static: Option<bool>,
}

#[derive(Serialize, Deserialize)]
pub struct DatabaseSpec {
    core: CoreSpec,
    sub: String,
}

#[derive(Serialize, Deserialize)]
pub struct MachineState {
    cores: HashMap<CoreId, CoreSpec>,
    dbs: HashMap<DatabaseId, CoreSpec>,
    output: DatabaseId,
}

// Request Structs

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Request {
    // Info Requests
    GetMachineState,

    // Hypercore Requests
    GetCore {
        spec: Option<CoreSpec>,
    },
    GetCoreSpec {
        id: CoreId,
    },
    GetCoreBlocks {
        id: CoreId,
        seqs: Vec<Seq>,
    },
    GetCoreLength {
        id: CoreId,
    },
    AppendCoreBlocks {
        id: CoreId,
        blocks: Vec<Block>,
    },

    // Hyperbee Requests
    GetPrimaryDatabase {
        sub: Option<String>,
    },
    GetDatabase {
        spec: Option<DatabaseSpec>,
    },
    GetDbRecord {
        id: DatabaseId,
        key: DatabaseKey,
    },
    PutDbRecord {
        id: DatabaseId,
        record: DatabaseRecord,
    },
}

// Public Functions

pub fn get_core(spec: Option<CoreSpec>) -> CoreId {
    host_request(Request::GetCore { spec })
}

pub fn get_core_blocks(id: CoreId, seqs: Vec<Seq>) -> Vec<Block> {
    host_request(Request::GetCoreBlocks { id, seqs })
}

pub fn get_core_length(id: CoreId) -> Seq {
    host_request(Request::GetCoreLength { id })
}

pub fn append_core_blocks(id: CoreId, blocks: Vec<Block>) -> u32 {
    host_request(Request::AppendCoreBlocks { id, blocks })
}

pub fn get_db(spec: Option<DatabaseSpec>) -> DatabaseId {
    host_request(Request::GetDatabase { spec })
}

pub fn get_primary_db(sub: Option<String>) -> DatabaseId {
    host_request(Request::GetPrimaryDatabase { sub })
}

pub fn get_db_record(id: DatabaseId, key: DatabaseKey) -> DatabaseRecord {
    host_request(Request::GetDbRecord { id, key })
}

pub fn put_db_record(id: DatabaseId, record: DatabaseRecord) -> u32 {
    host_request(Request::PutDbRecord { id, record })
}
