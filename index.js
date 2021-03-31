const { NanoresourcePromise: Nanoresource } = require('nanoresource-promise/emitter')
const Hyperbee = require('hyperbee')
const MachineRuntime = require('@hypermachines/runtime')
const lexint = require('lexicographic-integer')
const mutexify = require('mutexify/promise')
const camelCase = require('camelcase')
const sodium = require('sodium-native')
const cbor = require('cbor')

// System Namespace
const SYS_NAMESPACE = 'sys'
const CODE_KEY = 'code'
const PRIMARY_DB_KEY = 'db'

// Machine State Namespace
const STATE_NAMESPACE = 'state'
const INPUTS_PREFIX = 'inputs'
const OUTPUTS_PREFIX = 'outputs'
const TRACE_NAMESPACE = 'trace'

// Encoding/Decoding

function encode (obj) {
  return cbor.encodeOne(obj)
}

function decode (raw) {
  return cbor.decodeFirst(raw)
}

// Data Types

class CoreSpec {
  constructor (opts = {}) {
    this.key = opts.key
    this.length = opts.length
    this.writable = opts.writable
    this.isStatic = opts.isStatic
    // TODO: Should really have a separate DBSpec for this.
    this.sub = opts.sub
  }
  static async decode (raw) {
    const decoded = await decode(raw)
    return new CoreSpec(decoded)
  }
  encode () {
    return encode(this)
  }
}

//  Helpers

class HypercoreBatch {
  constructor (core, opts) {
    this.core = core
    this.isStatic = opts && opts.isStatic
    this._coreLength = this.isStatic ? opts.length : core.length
    this._buf = []
  }
  get writable () {
    return this.core.writable
  }
  get key () {
    return this.core.key
  }
  get length () {
    return this._coreLength + this._buf.length
  }
  get mutated () {
    return !!this._buf.length
  }
  get blocks () {
    return this._buf
  }
  get (seq) {
    if (seq > this._coreLength - 1) return this._buf[seq - this._coreLength - 1]
    return new Promise((resolve, reject) => {
      this.core.get(seq, (err, block) => {
        if (err) return reject(err)
        return resolve(block)
      })
    })
  }
  append (blocks) {
    this._buf.push(...blocks)
  }
  async flush () {
    await new Promise((resolve, reject) => {
      this.core.append(this._buf, err => {
        if (err) return reject(err)
        return resolve()
      })
    })
  }
  toSpec () {
    return new CoreSpec({
      key: this.key,
      length: this.length,
      writable: this.writable,
      isStatic: this.isStatic
    })
  }
}

class IdMap {
  constructor () {
    this.m = new Map()
    this.freeList = []
  }
  get size () {
    return this.m.size
  }
  next () {
    if (this.freeList.length) return this.freeList[this.freeList.length - 1]
    return this.m.size
  }
  set (value, id) {
    if (id === undefined) {
      id = this.freeList.length ? this.freeList.pop() : this.m.size
    }
    this.m.set(id, value)
    return id
  }
  get (k) {
    return this.m.get(k)
  }
  del (k) {
    this.m.delete(k)
    this.freeList.push(k)
  }
}

class StateBatch {
  constructor (corestore, trees) {
    this.corestore = corestore

    this.id = lexint.pack(this._getTransactionId(trees), 'hex')
    this.transactionState = trees.state.sub(this.id)
    this.inputs = this.transactionState.sub(INPUTS_PREFIX)
    this.outputs = this.transactionState.sub(OUTPUTS_PREFIX)

    this.rootBatch = trees.root.batch()
    this.traceBatch = trees.trace.batch({ batch: this.rootBatch })
    this.outputsBatch = this.outputs.batch({ batch: this.rootBatch })
    this.inputsBatch = this.inputs.batch({ batch: this.rootBatch })

    this._dbs = new Map()
    this._dbBatches = new Map()
    this._coreBatches = new Map()
    this._dbBatchesByHandle = new Map()
    this._coreBatchesByHandle = new Map()
  }

  _getTransactionId (trees) {
    return trees.root.feed.length
  }

  _hashBlocks (blocks) {
    const out = Buffer.allocUnsafe(sodium.crypto_generichash_BYTES)
    const hashes = blocks.map(block => {
      const out = Buffer.allocUnsafe(sodium.crypto_generichash_BYTES)
      sodium.crypto_generichash(out, block)
      return out
    })
    sodium.crypto_generichash_batch(out, hashes)
    return out
  }

  async computeOutputsBatch () {
    // Record the root hashes of every modified feed.
    const dbBatches = []
    for (const [handle, batch] of this._coreBatchesByHandle) {
      if (!batch.mutated) continue
      const hash = this._hashBlocks(batch.blocks)
      await this.outputsBatch.put('' + handle, hash)
    }
    for (const [handle, { root, batch }] of this._dbBatchesByHandle) {
      if (!root || !batch.length) continue
      const raw = await batch.getRawBatch()
      const hash = this._hashBlocks(raw)
      await this.outputsBatch.put('' + handle, hash)
      dbBatches.push({ batch, raw })
    }
    return dbBatches
  }

  async _getDbAndBatch (id, key, opts = {}) {
    let rootDb = this._dbs.get(key)
    let rootBatch = this._dbBatches.get(key)

    if (!rootDb) {
      // This will record the state of the input db's core in the inputs batch.
      const coreBatch = await this.getCore(id, { key, ...opts })
      key = coreBatch.key.toString('hex')
      rootDb = new Hyperbee(coreBatch.core, { extension: false, checkout: opts.version })
      rootBatch = rootDb.batch()
      let record = { root: true, batch: rootBatch }

      this._dbs.set(key, rootDb)
      this._dbBatches.set(key, record)
      this._dbBatchesByHandle.set(id, record)
    }

    return { rootDb, rootBatch }
  }

  async recordRpcCall (name, args) {
    // TODO: Better way?
    return this.traceBatch.put(this.id, await encode({
      name,
      args
    }))
  }

  async getDatabase (id, opts) {
    let key = opts && opts.key
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    const { rootDb, rootBatch } = await this._getDbAndBatch(id, key, opts)
    if (!key) key = rootDb.feed.key.toString('hex')

    const subKey = key + (opts.sub ? '-' + opts.sub : '')
    if (this._dbBatches.has(subKey)) return this._dbBatches.get(subKey).batch

    const db = opts.sub ? rootDb.sub(opts.sub) : rootDb
    const batch = db.batch({ batch: rootBatch })

    let record = { root: false, batch }
    this._dbBatches.set(subKey, record)
    this._dbBatchesByHandle.set(id, record)

    return batch
  }

  async getCore (id, opts) {
    let key = opts && opts.key
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    if (this._coreBatches.has(key)) return this._coreBatches.get(key)
    if (opts.version !== undefined) opts.length = opts.version

    const core = this.corestore.get({ key })
    await new Promise(resolve => core.ready(resolve))
    key = core.key.toString('hex')
    const batch = new HypercoreBatch(core, opts)
    const spec = batch.toSpec()

    await this.inputsBatch.put('' + id, await spec.encode())
    this._coreBatches.set(key, batch)
    this._coreBatchesByHandle.set(id, batch)

    return batch
  }

  async flush () {
    // TODO: This should ideally be atomic across cores/dbs.
    const dbBatches = await this.computeOutputsBatch()
    return Promise.all([
      ...[...this._coreBatches.values()].map(b => b.flush()),
      ...[...dbBatches].map(({ batch, raw }) => batch.flush(raw)),
      this.rootBatch.flush()
    ])
  }

  destroy () {
    this.rootBatch.destroy()
    for (const { root, batch } of this._dbBatches.values()) {
      if (!root) continue
      batch.destroy()
    }
  }
}

class RequestState {
  constructor (corestore, trees, writable, validationInputs) {
    this.corestore = corestore

    this.trees = trees
    this.writable = writable
    this.validationInputs = validationInputs
    this.batch = new StateBatch(this.corestore, this.trees)

    this._handles = new IdMap()
  }

  recordRpcCall (name, args) {
    return this.batch.recordRpcCall(name, args)
  }

  dispatch (message) {
    switch (message.type) {
      case 'GetPrimaryDatabase':
        return this._getPrimaryDatabase(message)
      case 'GetCore':
        return this._getCore(message)
      case 'AppendCoreBlocks':
        return this._coreAppend(message)
      case 'GetCoreBlocks':
        return this._coreGet(message)
      case 'GetCoreLength':
        return this._coreGetLength(message)
      case 'GetDbRecord':
        return this._databaseGet(message)
      case 'PutDbRecord':
        return this._databasePut(message)
      default:
        throw new Error('Unhandled message type.')
    }
  }

  flush () {
    // If this is a replay, do not save outputs.
    if (!this.validationInputs) return this.batch.flush()
    return this.destroy()
  }

  destroy () {
    return this.batch.destroy()
  }

  /**
   * Used during validation
   */

  getOutputs () {
    return this.batch.outputsBatch
  }

  computeOutputsBatch () {
    return this.batch.computeOutputsBatch()
  }

  // RPC Helpers

  async _getNextValidationVersion () {
    const id = this._handles.next()
    const specNode = await this.validationInputs.get('' + id)
    if (!specNode) throw new Error('Could not find core spec for ID', id)
    const spec = await decode(specNode.value)
    return spec.length
  }

  // RPC Methods

  async _getPrimaryDatabase ({ sub }) {
    const key = (await this.trees.system.get(PRIMARY_DB_KEY)).value
    return this._getDatabase({ spec: { key, sub } })
  }

  async _getCore ({ spec }) {
    const id = this._handles.next()
    const version = this.validationInputs ? await this._getNextValidationVersion() : spec && spec.length
    const core = await this.batch.getCore(id, {
      ...spec,
      version,
      isStatic: version !== null
    })
    this._handles.set(core, id)
    return encode(id)
  }

  async _getDatabase ({ spec }) {
    const id = this._handles.next()
    const version = this.validationInputs ? await this._getNextValidationVersion() : spec && spec.length
    const db = await this.batch.getDatabase(id, {
      ...spec,
      version,
    })
    this._handles.set(db, id)
    return encode(id)
  }

  async _coreGet ({ id, seqs }) {
    const core = this._handles.get(id)
    if (!core) throw new Error('Invalid core.')
    const blocks = await Promise.all(seqs.map(s => core.get(s)))
    return encode(blocks)
  }

  async _coreAppend ({ id, blocks }) {
    const core = this._handles.get(id)
    if (!core) throw new Error('Invalid core.')
    core.append(blocks)
    return encode(0)
  }

  async _coreGetLength ({ id }) {
    const core = this._handles.get(id)
    if (!core) throw new Error('Invalid core.')
    return encode(core.length)
  }
 
  async _databasePut ({ id, record }) {
    const db = this._handles.get(id)
    if (!db) throw new Error('Invalid database.')
    await db.put(record.key, record.value) 
    return encode(0)
  }

  async _databaseGet ({ id, key }) {
    const db = this._handles.get(id)
    if (!db) throw new Error('Invalid database.')
    const node = await db.get(key)
    return encode({ key, value: node && node.value })
  }
}

class Hypermachine extends Nanoresource {
  constructor (factory, tree, opts = {}) {
    super()
    this.factory = factory
    this.corestore = factory.corestore.namespace()
    this.networker = factory.networker
    this.key = tree.feed.key

    this.trees = {
      root: tree,
      system: tree.sub(SYS_NAMESPACE),
      trace: tree.sub(TRACE_NAMESPACE),
      state: tree.sub(STATE_NAMESPACE)
    }
    this.ready = this.start.bind(this)

    // Set in _open
    this._lock = mutexify()
    this._code = null
    this._instance = null
    this._noInit = !!opts.noInit
    this._noTrace = !!opts.noTrace

    // Set during RPC calls.
    this._requestState = null
    // Set during validation.
    this._validationInputs = null
  }
  
  // Nanoresource Methods

  async _open () {
    const codeNode = await this.trees.system.get(CODE_KEY)
    if (!codeNode || !codeNode.value) throw new Error('Invalid hypermachine (code not found).')
    this._code = codeNode.value
    this._instance = new MachineRuntime(this._code, {
      onHostcall: this._onHostcall.bind(this)
    })
    await this._instance.open()
    this._createRpcMethods()
  }

  async _close () {
    await this._instance.close()
    await this.trees.root.feed.close()
    this.emit('close')
  }

  // Private Methods

  _createRpcMethods () {
    const props = Object.getOwnPropertyNames(this._instance)
    const readRpcs = props.filter(prop => prop.startsWith('read') && !(prop === 'ready'))
    const writeRpcs = props.filter(prop => prop.startsWith('write'))
    this.rpc = {}
    if (this.trees.root.feed.writable) {
      for (const rpc of writeRpcs) {
        const methodName = camelCase(rpc.slice(5))
        this.rpc[methodName] = this._createRpcMethod(methodName, this._instance[rpc].bind(this._instance), true)
      }
    }
    for (const rpc of readRpcs) {
      const methodName = camelCase(rpc.slice(4))
      this.rpc[methodName] = this._createRpcMethod(methodName, this._instance[rpc].bind(this._instance), false)
    }
  }

  _createRpcMethod (name, func, writable) {
    return async (args) => {
      const release = await this._lock()
      this._requestState = new RequestState(this.corestore, this.trees, writable, this._validationInputs)
      try {
        const result = await func(args)
        if (this._validationInputs) {
          await this._requestState.computeOutputsBatch()
          return this._requestState
        }
        if (writable && !this._noTrace) {
          // The RPC call must be added to the batch last so we can do a checkout to its seq during validation.
          await this._requestState.recordRpcCall(name, args)
          await this._requestState.flush()
        } else {
          this._requestState.destroy()
        }
        return decode(result)
      } finally {
        this._requestState = null
        this._validationInputs = null
        release()
      }
    }
  }

  // Hostcall Dispatcher

  async _onHostcall (_, args) {
    const message = await decode(args)
    if (!this._requestState) throw new Error('Hostcall can only be handled inside an RPC request.')
    if (!message.type) throw new Error('Malformed message.')
    return this._requestState.dispatch(message)
  }

  // Public Methods

  async _validateOutputs (original, replayed) {
    const originalOutputs = await collect(original.createReadStream())
    const replayedOutputs = await collect(replayed.createReadStream())
    if (originalOutputs.length !== replayedOutputs.length) return false
    for (let i = 0; i < originalOutputs.length; i++) {
      const sourceNode = originalOutputs[i]
      const replayNode = replayedOutputs[i]
      if (!sourceNode.key.equals(replayNode.key)) return false
      if (!sourceNode.value.equals(replayNode.value)) return false
    }
    return true
  }

  async validate () {
    if (this._noTrace) throw new Error('Cannot validate a machine that has disabled tracing.')
    for await (let { key, seq, value } of this.trees.trace.createReadStream()) {
      key = key.toString('utf8')
      const { name, args } = await decode(value)

      // TODO: Checkouts with parent batches have a bug -- investigate.
      const rootCheckout = this.trees.root.checkout(seq + 2)
      const transactionCheckout = rootCheckout.sub(STATE_NAMESPACE).sub(key)
      const validationInputs = transactionCheckout.sub(INPUTS_PREFIX)
      const validationOutputs = transactionCheckout.sub(OUTPUTS_PREFIX)

      this._setValidationInputs(validationInputs)
      const requestState = await this.rpc[name](args)
      const replayedOutputs = requestState.getOutputs()
      if (!(await this._validateOutputs(validationOutputs, replayedOutputs))) return false
      await requestState.destroy()
    }
    return true
  }

  _setValidationInputs (inputs) {
    this._validationInputs = inputs
  }

  /**
   * Start the Hypermachine
   * 
   * Options will be serialized and passed to the machine's init function.
   * 
   * @param {Object} opts 
   */
  async start (opts = {}) {
    await this.open()
    const firstRun = this.trees.root.feed.length === 3
    if (!firstRun || this._noInit) return
    if (this.rpc.init) await this.rpc.init(await encode(opts))
  }
}

module.exports = class Hypermachines extends Nanoresource {
  constructor (corestore, networker, opts = {}) {
    super()
    this.corestore = corestore
    this.networker = networker
    this._machines = []
  }

  // Nanoresource Methods

  async _open () {
    await this.corestore.ready()
    await this.networker.listen()
  }

  async _close () {
    for (const machine of this._machines) {
      await machine.close()
    }
    await this.networker.close()
    await this.corestore.close()
  }

  _addMachine (machine) {
    this._machines.push(machine)
    machine.once('close', () => {
      const idx = this._machines.indexOf(machine)
      if (idx !== -1) this._machines.splice(idx, 1)
    })
  }

  // Public Methods

  /**
   * Creates a new Hypermachine.
   * 
   * @param {String} code: WASM bytecode
   * @param {Object} opts: Machine options
   */
  async create (code, opts = {}) {
    await this.open()
    const namespace = this.corestore.namespace()

    // Populate the machine's database with the correct initial records.
    const tree = new Hyperbee(namespace.get(), { sep: '!' })
    await tree.sub(SYS_NAMESPACE).put(CODE_KEY, code)
    const primaryDb = new Hyperbee(namespace.get())
    await primaryDb.ready()
    await tree.sub(SYS_NAMESPACE).put(PRIMARY_DB_KEY, primaryDb.feed.key)

    const machine = new Hypermachine(this, tree, opts)
    this._addMachine(machine)

    return machine
  }

  /**
   * Duplicate an existing machine, but with an empty trace.
   * 
   * @param {Buffer | String | Object } source - An existing Hypermachine, or a key.
   * @param {Object} opts - Machine options
   */
  async clone (source, opts = {}) {
    await this.open()
    const namespace = this.corestore.namespace()

    if (Buffer.isBuffer(source) || typeof source === 'string') {
      const sourceCore = namespace.get(key)
      const tree = new Hyperbee(sourceCore)
      source = new Hypermachine(this, tree, opts)
    }

    const codeNode = await source.trees.system.get(CODE_KEY)
    if (!codeNode) throw new Error('Cannot clone -- code not found.')

    return this.create(codeNode.value, opts)
  }

  /**
   * Get an existing machine by key.
   * 
   * @param {Buffer} key - The Hypercore Key
   * @param {*} opts - Machine options
   */
  async get (key, opts = {}) {
    await this.open()
    const namespace = this.corestore.namespace()

    const tree = new Hyperbee(namespace.get({ key }))
    const machine = new Hypermachine(this, tree, opts)
    this._addMachine(machine)

    return machine
  }
}

function collect (stream) {
  return new Promise((resolve, reject) => {
    const entries = []
    stream.on('data', d => entries.push(d))
    stream.on('end', () => resolve(entries))
    stream.on('error', err => reject(err))
    stream.on('close', () => reject(new Error('Premature close')))
  })
}
