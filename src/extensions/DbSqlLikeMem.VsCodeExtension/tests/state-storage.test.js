const test = require('node:test');
const assert = require('node:assert/strict');
const { persistConnectionState, restoreConnectionSecrets, withoutConnectionSecrets } = require('../out/state-storage.js');

function createStorage(initialState, initialSecrets = []) {
  let current = structuredClone(initialState);
  const vault = new Map(initialSecrets);
  return {
    globalState: {
      get: () => structuredClone(current),
      update: async (_key, state) => { current = structuredClone(state); }
    },
    secrets: {
      get: async key => vault.get(key),
      store: async (key, value) => { vault.set(key, value); },
      delete: async key => { vault.delete(key); }
    },
    readState: () => current,
    vault
  };
}

test('legacy credentials migrate to the vault and survive reload without plaintext state', async () => {
  const legacy = { connections: [{ id: 'erp', connectionString: 'Password=private;', name: 'ERP' }] };
  const storage = createStorage(legacy);
  await restoreConnectionSecrets(storage, structuredClone(legacy));
  assert.equal(storage.readState().connections[0].connectionString, '');
  assert.equal(storage.vault.get('dbSqlLikeMem.connection.erp'), 'Password=private;');
  const restored = await restoreConnectionSecrets(storage, structuredClone(storage.readState()));
  assert.equal(restored.connections[0].connectionString, 'Password=private;');
});

test('vault failure preserves the legacy state so migration can be retried', async () => {
  const legacy = { connections: [{ id: 'erp', connectionString: 'Password=private;' }] };
  const storage = createStorage(legacy);
  storage.secrets.store = async () => { throw new Error('vault unavailable'); };
  await assert.rejects(restoreConnectionSecrets(storage, structuredClone(legacy)), /vault unavailable/);
  assert.deepEqual(storage.readState(), legacy);
});

test('removing a connection deletes only its secret', async () => {
  const storage = createStorage({ connections: [{ id: 'old', connectionString: '' }] }, [
    ['dbSqlLikeMem.connection.old', 'old-secret'],
    ['unrelated', 'keep']
  ]);
  await persistConnectionState(storage, { connections: [] });
  assert.equal(storage.vault.has('dbSqlLikeMem.connection.old'), false);
  assert.equal(storage.vault.get('unrelated'), 'keep');
});

test('exports redact secrets without mutating the active connections or mappings', () => {
  const state = { connections: [{ id: 'erp', name: 'ERP', connectionString: 'private' }], mappingConfigurations: [{ connectionId: 'erp', mappings: [] }] };
  const exported = withoutConnectionSecrets(state);
  assert.equal(JSON.stringify(exported).includes('private'), false);
  assert.equal(state.connections[0].connectionString, 'private');
  assert.deepEqual(exported.mappingConfigurations, state.mappingConfigurations);
});

test('imported connection without credentials does not inherit an unrelated saved secret with the same id', async () => {
  const storage = createStorage({ connections: [{ id: 'erp', connectionString: '' }] }, [['dbSqlLikeMem.connection.erp', 'old-secret']]);
  await persistConnectionState(storage, { connections: [{ id: 'erp', connectionString: '' }] });
  assert.equal(storage.vault.has('dbSqlLikeMem.connection.erp'), false);
});
