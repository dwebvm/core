#[macro_use]
extern crate serde_derive;

use blake2::{Blake2b, Digest};
use hypermachines::*;
use hypermachines_sys::{host_request, read_arguments, rpc_response};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use serde_cbor::{from_slice, to_vec};

// Start Boilerplate
// TODO: Figure out how to extract this.

extern "C" {
  pub fn machine_hostcall(ptr: *mut u8, length: usize) -> *mut u8;
}

#[no_mangle]
pub extern "C" fn malloc(size: usize) -> *mut u8 {
  hypermachines_sys::malloc(size)
}

#[no_mangle]
pub extern "C" fn free(ptr: *mut u8, size: usize) {
  hypermachines_sys::free(ptr, size)
}

// End Boilerplate

fn get_output_core() -> CoreId {
  let output_db = get_primary_db(None);
  let output_spec: CoreSpec = from_slice(
    &get_db_record(output_db, format!("output_core_spec"))
      .value
      .unwrap(),
  )
  .unwrap();
  get_core(Some(output_spec))
}

fn get_last_block(output_core: CoreId) -> Option<ByteBuf> {
  match get_core_length(output_core) {
    0 => None,
    len @ _ => Some(
      get_core_blocks(output_core, vec![len - 1])
        .first()
        .unwrap()
        .clone(),
    ),
  }
}

fn update_hash(output_core: CoreId, input: Vec<u8>) -> Vec<u8> {
  let mut hasher = Blake2b::new();
  let last_block = get_last_block(output_core);
  if let None = last_block {
    hasher.update(input)
  } else {
    hasher.update(last_block.unwrap());
    hasher.update(input);
  }
  hasher.finalize().to_vec()
}

#[no_mangle]
pub extern "C" fn rpc_write_init(args_ptr: *mut u8, args_length: usize) -> *mut u8 {
  let output_db = get_primary_db(None);
  // Check if an output core has already been created.
  let arg_vec: Vec<u8> = read_arguments(args_ptr, args_length);
  let output_spec: CoreSpec = from_slice(&arg_vec).unwrap();
  put_db_record(
    output_db,
    DatabaseRecord {
      key: format!("output_core_spec"),
      value: Some(ByteBuf::from(to_vec(&output_spec).unwrap())),
    },
  );
  rpc_response(0)
}

#[no_mangle]
pub extern "C" fn rpc_write_append(args_ptr: *mut u8, args_length: usize) -> *mut u8 {
  let value: Vec<u8> = read_arguments(args_ptr, args_length);
  let output_core = get_output_core();
  let hash = update_hash(output_core, value);
  append_core_blocks(output_core, vec![ByteBuf::from(hash)]);
  rpc_response(0)
}

#[no_mangle]
pub extern "C" fn rpc_read_get_hash(_args_ptr: *mut u8, _args_length: usize) -> *mut u8 {
  let output_core = get_output_core();
  let last_block = get_last_block(output_core);
  match last_block {
    None => rpc_response(""),
    res @ _ => rpc_response(res),
  }
}
