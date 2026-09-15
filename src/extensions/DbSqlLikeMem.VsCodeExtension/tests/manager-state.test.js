const test = require('node:test');
const assert = require('node:assert/strict');
const { createManagerViewState, serializeWebviewData, readMappingNamespace } = require('../out/manager-state.js');

test('manager data exposes credential availability without sending secrets to the webview', () => {
  const state = {
    connections: [
      { id: 'erp', name: 'ERP', databaseType: 'SqlServer', databaseName: 'erp', connectionString: 'Password=private' },
      { id: 'new', name: 'New', databaseType: 'Sqlite', databaseName: 'main', connectionString: '' }
    ],
    mappingConfigurations: [{ connectionId: 'erp', mappings: [
      { objectType: 'Table', targetFolder: 'Tables', fileSuffix: 'Factory', namespace: 'App.Tables' }
    ] }]
  };
  const result = createManagerViewState(state);
  assert.equal(JSON.stringify(result).includes('Password=private'), false);
  assert.equal(Object.hasOwn(result.connections[0], 'connectionString'), false);
  assert.equal(result.connections[0].hasCredentials, true);
  assert.equal(result.connections[1].hasCredentials, false);
  result.mappingConfigurations[0].mappings[0].namespace = 'Changed';
  assert.equal(state.mappingConfigurations[0].mappings[0].namespace, 'App.Tables');
});

test('imported names and IDs cannot close the manager data script element', () => {
  const value = { selectedConnectionId: '</script><script>alert(1)</script>', name: 'A & B <ERP>' };
  const json = serializeWebviewData(value);
  assert.equal(json.includes('<'), false);
  assert.deepEqual(JSON.parse(json), value);
});

test('mapping namespaces remain independent and can be cleared explicitly', () => {
  const payload = { tableNamespace: ' App.Tables ', viewNamespace: 'App.Views', functionNamespace: '', namespace: 'Legacy' };
  assert.equal(readMappingNamespace(payload, 'Table'), 'App.Tables');
  assert.equal(readMappingNamespace(payload, 'View'), 'App.Views');
  assert.equal(readMappingNamespace(payload, 'Function'), undefined);
});

test('legacy mapping payloads retain the shared namespace fallback', () => {
  assert.equal(readMappingNamespace({ namespace: ' Legacy.App ' }, 'Sequence'), 'Legacy.App');
  assert.equal(readMappingNamespace({}, 'Sequence'), undefined);
});
