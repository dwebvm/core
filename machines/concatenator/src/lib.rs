#[macro_use]
extern crate serde_derive;

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
pub extern "C" fn rpc_write_concatenate(args_ptr: *mut u8, args_length: usize) -> *mut u8 {
  let value = String::from_utf8(read_arguments(args_ptr, args_length)).unwrap();
  let output_core = get_output_core();
  let output_core_length = get_core_length(output_core);

  if output_core_length == 0 {
    append_core_blocks(output_core, vec![ByteBuf::from(value.into_bytes())]);
  } else {
    let mut blocks = get_core_blocks(output_core, vec![output_core_length - 1]);
    let last_block = blocks.first_mut().unwrap();
    last_block.append(&mut value.into_bytes());
    append_core_blocks(output_core, vec![ByteBuf::from(last_block.to_vec())]);
  }
  rpc_response(0)
}

#[no_mangle]
pub extern "C" fn rpc_read_get_concatenation(_args_ptr: *mut u8, _args_length: usize) -> *mut u8 {
  let output_core = get_output_core();
  let output_core_length = get_core_length(output_core);

  if output_core_length == 0 {
    rpc_response("")
  } else {
    let blocks = get_core_blocks(output_core, vec![output_core_length - 1]);
    let last_block = blocks.first().unwrap();
    rpc_response(String::from_utf8(last_block.to_vec()).unwrap())
  }
}
