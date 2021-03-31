const fs = require('fs').promises
const Corestore = require('corestore')
const Networker = require('@corestore/networker')
const ram = require('random-access-memory')
const Hypermachines = require('../..')

async function create (opts = {}) {
  const store = new Corestore(ram, { sparse: true })
  await store.ready()
  const networker = new Networker(store)

  const machines = new Hypermachines(store, networker)
  await machines.open()

  var machine = null
  if (opts.key) {
    machine = await machines.get(opts.key, opts)
  } else if (opts.path) {
    const code = await fs.readFile(opts.path)
    machine = await machines.create(code, opts)
  } else if (opts.clone) {
    machine = await machines.clone(opts.clone, opts)
  }

  return {
    machines,
    machine,
    store,
    cleanup: async () => {
      await networker.close()
      await machines.close()
    }
  }
}

module.exports = {
  create
}
