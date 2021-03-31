const p = require('path')
const test = require('tape')
const sodium = require('sodium-native')
const { create } = require('./helpers/create')

const CONCATENATOR_PATH = p.join(__dirname, 'machines', 'concatenator.wasm')
const HASHER_PATH = p.join(__dirname, 'machines', 'hasher.wasm')

test('simple concatenator', async t => {
  const { machine, store, cleanup } = await create({ path: CONCATENATOR_PATH })
  const outputCore = store.get()
  await machine.start({ key: outputCore.key })

  await machine.rpc.concatenate('hello')
  await machine.rpc.concatenate('world')
  await machine.rpc.concatenate('hello')
  const result = await machine.rpc.getConcatenation()
  t.same(result, 'helloworldhello')

  await cleanup()
  t.end()
})

test('can validate a concatenator', async t => {
  const { machine, store, cleanup } = await create({ path: CONCATENATOR_PATH })
  const outputCore = store.get()
  await machine.start({ key: outputCore.key })

  await machine.rpc.concatenate('hello')
  await machine.rpc.concatenate('world')
  await machine.rpc.concatenate('hello')
  const result = await machine.rpc.getConcatenation()
  t.same(result, 'helloworldhello')

  // Replay the machine and ensure that the output hashes match after every step.
  t.true(await machine.validate())

  await cleanup()
  t.end()
})

test('can validate a long concatenator', async t => {
  const LENGTH = 100

  const { machine, store, cleanup } = await create({ path: CONCATENATOR_PATH })
  const outputCore = store.get()
  await machine.start({ key: outputCore.key })

  let ref = []
  for (let i = 0; i < LENGTH; i++) {
    await machine.rpc.concatenate('' + i)
    ref.push('' + i)
  }

  const result = await machine.rpc.getConcatenation()
  t.same(result, ref.join(''))
  t.true(await machine.validate())

  await cleanup()
  t.end()
})

test('can compute a rolling hash', async t => {
  const LENGTH = 20

  const { machine, store, cleanup } = await create({ path: HASHER_PATH })
  const outputCore = store.get()
  await machine.start({ key: outputCore.key })

  let arr = []
  for (let i = 0; i < LENGTH; i++) {
    const val = Buffer.from('' + i)
    arr.push(val)
    await machine.rpc.append(val)
  }

  const reference = rollingHash(arr)
  const machineHash = await machine.rpc.getHash()
  t.same(reference, machineHash)

  t.true(await machine.validate())

  await cleanup()
  t.end()
})

function rollingHash (arr) {
  const out = Buffer.allocUnsafe(sodium.crypto_generichash_BYTES_MAX)
  for (let i = 0; i < arr.length; i++) {
    const state = Buffer.allocUnsafe(sodium.crypto_generichash_STATEBYTES)
    sodium.crypto_generichash_init(state, null, out.length)
    if (i !== 0) sodium.crypto_generichash_update(state, out)
    sodium.crypto_generichash_update(state, arr[i])
    sodium.crypto_generichash_final(state, out)
  }
  return out
}
