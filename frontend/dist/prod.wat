(module
 (type $i32_i32_=>_i32 (func (param i32 i32) (result i32)))
 (memory $0 0)
 (global $src/assembly/wasm/num i32 (i32.const 2))
 (export "add" (func $src/assembly/wasm/add))
 (export "num" (global $src/assembly/wasm/num))
 (export "memory" (memory $0))
 (func $src/assembly/wasm/add (param $0 i32) (param $1 i32) (result i32)
  local.get $0
  local.get $1
  i32.add
 )
)
