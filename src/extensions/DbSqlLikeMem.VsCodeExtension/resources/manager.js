(() => {
  const vscode = acquireVsCodeApi();
  const initial = JSON.parse(document.getElementById('managerData').textContent);
  const labels = initial.labels;
  const types = initial.objectTypes;
  const byId = id => document.getElementById(id);
  const drafts = new Map();
  const connectionDrafts = new Map();
  let state = initial;
  let selectedId = initial.selectedConnectionId || '';
  let busy = false;
  let pending = false;
  let activeRequestId = '';
  let feedback = { kind: 'info', message: labels.ready };

  function showFeedback(kind, message) {
    feedback = { kind, message };
    renderFeedback();
  }

  function renderFeedback() {
    const element = byId('feedback');
    element.className = 'feedback ' + (busy || pending ? 'busy' : feedback.kind);
    element.textContent = busy || pending ? labels.working : feedback.message;
    element.setAttribute('aria-live', feedback.kind === 'error' ? 'assertive' : 'polite');
  }

  function updateControls() {
    const blocked = busy || pending;
    byId('connectionFields').disabled = blocked;
    byId('mappingFields').disabled = blocked || !state.connections.length;
    document.querySelectorAll('[data-connection-action]').forEach(button => { button.disabled = blocked; });
    byId('emptyMapping').hidden = state.connections.length > 0;
    byId('mappingCards').hidden = !state.connections.length;
    byId('resetMapping').disabled = blocked || !drafts.has(selectedId);
    byId('mappingStatus').textContent = drafts.has(selectedId) ? labels.unsaved : '';
    const connectionDirty = connectionDrafts.has(byId('connectionIdHidden').value);
    byId('connectionStatus').textContent = connectionDirty ? labels.unsaved : '';
    byId('resetConnection').disabled = blocked || !connectionDirty;
    document.querySelector('main').setAttribute('aria-busy', String(blocked));
    renderFeedback();
  }

  function submit(payload) {
    if (busy || pending) {
      return;
    }
    pending = true;
    activeRequestId = Date.now() + '-' + Math.random().toString(36).slice(2);
    updateControls();
    vscode.postMessage({ ...payload, requestId: activeRequestId });
  }

  function readMapping() {
    const result = {};
    for (const type of types) {
      const prefix = type.toLowerCase();
      result[type] = {
        targetFolder: byId(prefix + 'Folder').value,
        fileSuffix: byId(prefix + 'Suffix').value,
        namespace: byId(prefix + 'Namespace').value
      };
    }
    return result;
  }

  function populateMapping() {
    const configuration = state.mappingConfigurations.find(mapping => mapping.connectionId === selectedId);
    const saved = Object.fromEntries((configuration?.mappings || []).map(mapping => [mapping.objectType, mapping]));
    const values = drafts.get(selectedId) || saved;
    for (const type of types) {
      const prefix = type.toLowerCase();
      const mapping = values[type];
      byId(prefix + 'Folder').value = mapping?.targetFolder ?? 'src/Generated';
      byId(prefix + 'Suffix').value = mapping?.fileSuffix ?? 'Factory';
      byId(prefix + 'Namespace').value = mapping?.namespace ?? '';
    }
    byId('mappingForm').querySelectorAll('input').forEach(input => input.setCustomValidity(''));
    updateControls();
  }

  function restoreConnectionDraft() {
    const draft = connectionDrafts.get(byId('connectionIdHidden').value);
    if (draft) {
      for (const [id, value] of Object.entries(draft)) {
        byId(id).value = value;
      }
    }
    byId('connectionForm').querySelectorAll('input').forEach(input => input.setCustomValidity(''));
    updateControls();
  }

  function resetConnection(focus = true) {
    byId('connectionForm').reset();
    byId('connectionIdHidden').value = '';
    byId('connectionString').required = true;
    byId('connectionString').type = 'password';
    byId('toggleSecret').textContent = labels.showSecret;
    byId('toggleSecret').setAttribute('aria-pressed', 'false');
    byId('editorMode').textContent = labels.addHint;
    byId('credentialsHint').textContent = labels.requiredHint;
    restoreConnectionDraft();
    if (focus) {
      byId('name').focus();
    }
  }

  function editConnection(connection) {
    byId('connectionIdHidden').value = connection.id;
    byId('name').value = connection.name;
    byId('databaseType').value = connection.databaseType;
    byId('databaseName').value = connection.databaseName;
    byId('connectionString').value = '';
    byId('connectionString').required = !connection.hasCredentials;
    byId('connectionString').type = 'password';
    byId('toggleSecret').textContent = labels.showSecret;
    byId('toggleSecret').setAttribute('aria-pressed', 'false');
    byId('editorMode').textContent = labels.editHint + ': ' + connection.name;
    byId('credentialsHint').textContent = connection.hasCredentials ? labels.keepCredentials : labels.requiredHint;
    restoreConnectionDraft();
    byId('name').focus();
    byId('connectionHeading').scrollIntoView({ block: 'nearest' });
  }

  function renderState() {
    const options = state.connections.map(connection => {
      const option = document.createElement('option');
      option.value = connection.id;
      option.textContent = connection.name + ' (' + connection.databaseType + ' / ' + connection.databaseName + ')';
      return option;
    });
    byId('connectionId').replaceChildren(...options);
    if (!state.connections.some(connection => connection.id === selectedId)) {
      selectedId = state.connections[0]?.id || '';
    }
    byId('connectionId').value = selectedId;
    for (const id of drafts.keys()) {
      if (!state.connections.some(connection => connection.id === id)) {
        drafts.delete(id);
      }
    }
    const editedId = byId('connectionIdHidden').value;
    if (editedId && !state.connections.some(connection => connection.id === editedId)) {
      showFeedback('error', labels.missingConnection);
    }
    const rows = state.connections.map(connection => {
      const row = document.createElement('tr');
      for (const value of [connection.name, connection.databaseType, connection.databaseName]) {
        const cell = document.createElement('td');
        cell.textContent = value;
        row.append(cell);
      }
      const actions = document.createElement('td');
      const actionGroup = document.createElement('div');
      actionGroup.className = 'button-row';
      for (const [label, action] of [
        [labels.editConnection, () => editConnection(connection)],
        [labels.deleteConnection, () => submit({ type: 'removeConnection', connectionId: connection.id })]
      ]) {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'secondary';
        button.textContent = label;
        button.setAttribute('aria-label', label + ': ' + connection.name);
        button.dataset.connectionAction = 'true';
        button.addEventListener('click', action);
        actionGroup.append(button);
      }
      actions.append(actionGroup);
      row.append(actions);
      return row;
    });
    byId('connectionRows').replaceChildren(...rows);
    byId('connectionCount').textContent = labels.connectionCount + ': ' + state.connections.length;
    byId('connectionsTable').hidden = state.connections.length === 0;
    byId('emptyConnections').hidden = state.connections.length > 0;
    populateMapping();
  }

  for (const type of initial.databaseTypes) {
    const option = document.createElement('option');
    option.value = type;
    option.textContent = type;
    byId('databaseType').append(option);
  }

  document.querySelectorAll('input[required]').forEach(input => {
    input.addEventListener('input', () => input.setCustomValidity(!input.required || input.value.trim() ? '' : labels.requiredField));
  });
  byId('connectionForm').addEventListener('submit', event => {
    event.preventDefault();
    submit({
      type: 'saveConnection',
      connectionId: byId('connectionIdHidden').value,
      name: byId('name').value.trim(),
      databaseType: byId('databaseType').value,
      databaseName: byId('databaseName').value.trim(),
      connectionString: byId('connectionString').value.trim()
    });
  });
  byId('connectionForm').addEventListener('input', () => {
    connectionDrafts.set(byId('connectionIdHidden').value, Object.fromEntries(
      ['name', 'databaseType', 'databaseName', 'connectionString'].map(id => [id, byId(id).value])
    ));
    updateControls();
  });
  byId('newConnection').addEventListener('click', () => resetConnection());
  byId('resetConnection').addEventListener('click', () => {
    const id = byId('connectionIdHidden').value;
    connectionDrafts.delete(id);
    const connection = state.connections.find(item => item.id === id);
    if (connection) {
      editConnection(connection);
    } else {
      resetConnection();
    }
  });
  byId('toggleSecret').addEventListener('click', () => {
    const visible = byId('connectionString').type === 'password';
    byId('connectionString').type = visible ? 'text' : 'password';
    byId('toggleSecret').textContent = visible ? labels.hideSecret : labels.showSecret;
    byId('toggleSecret').setAttribute('aria-pressed', String(visible));
  });
  byId('connectionId').addEventListener('change', () => {
    selectedId = byId('connectionId').value;
    populateMapping();
  });
  byId('mappingCards').addEventListener('input', () => {
    drafts.set(selectedId, readMapping());
    updateControls();
  });
  byId('resetMapping').addEventListener('click', () => {
    drafts.delete(selectedId);
    populateMapping();
  });
  byId('mappingForm').addEventListener('submit', event => {
    event.preventDefault();
    const values = readMapping();
    const payload = { type: 'saveMapping', connectionId: selectedId };
    for (const type of types) {
      const prefix = type.toLowerCase();
      payload[prefix + 'Folder'] = values[type].targetFolder.trim();
      payload[prefix + 'Suffix'] = values[type].fileSuffix.trim();
      payload[prefix + 'Namespace'] = values[type].namespace.trim();
    }
    submit(payload);
  });
  window.addEventListener('message', event => {
    const message = event.data;
    if (!message || typeof message !== 'object') {
      return;
    }
    if (message.type === 'state') {
      state = message.state;
      renderState();
    } else if (message.type === 'busy') {
      busy = !!message.busy;
      pending = false;
      updateControls();
    } else if (message.type === 'feedback') {
      showFeedback(message.kind, message.message);
    } else if (message.type === 'saved') {
      if (message.requestId !== activeRequestId) {
        return;
      }
      activeRequestId = '';
      if (message.kind === 'connection') {
        connectionDrafts.delete(byId('connectionIdHidden').value);
        resetConnection(false);
        selectedId = message.connectionId;
      } else if (message.kind === 'mapping') {
        drafts.delete(message.connectionId);
      } else if (message.kind === 'remove') {
        drafts.delete(message.connectionId);
        connectionDrafts.delete(message.connectionId);
        if (byId('connectionIdHidden').value === message.connectionId) {
          resetConnection(false);
        }
      }
      showFeedback('success', message.message);
    }
  });
  resetConnection(false);
  renderState();
  vscode.postMessage({ type: 'ready' });
})();
