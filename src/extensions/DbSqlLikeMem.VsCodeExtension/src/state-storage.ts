interface ConnectionState {
  connections: Array<{ id: string; connectionString: string }>;
}

interface StateStorage {
  globalState: {
    get<T>(key: string): T | undefined;
    update(key: string, value: unknown): PromiseLike<void>;
  };
  secrets: {
    get(key: string): PromiseLike<string | undefined>;
    store(key: string, value: string): PromiseLike<void>;
    delete(key: string): PromiseLike<void>;
  };
}

const stateKey = 'dbSqlLikeMem.state';
const secretKey = (id: string): string => `dbSqlLikeMem.connection.${id}`;

export function withoutConnectionSecrets<T extends ConnectionState>(state: T): T {
  return { ...state, connections: state.connections.map(connection => ({ ...connection, connectionString: '' })) };
}

export async function restoreConnectionSecrets<T extends ConnectionState>(storage: StateStorage, state: T): Promise<T> {
  for (const connection of state.connections) {
    // A legacy plaintext value takes priority until migration has completed.
    connection.connectionString ||= await storage.secrets.get(secretKey(connection.id)) ?? '';
  }
  await persistConnectionState(storage, state);
  return state;
}

export async function persistConnectionState<T extends ConnectionState>(storage: StateStorage, state: T): Promise<void> {
  const previous = storage.globalState.get<ConnectionState>(stateKey);
  // Save secrets first: a vault failure must not erase legacy credentials.
  for (const connection of state.connections) {
    if (connection.connectionString) {
      await storage.secrets.store(secretKey(connection.id), connection.connectionString);
    }
  }
  await storage.globalState.update(stateKey, withoutConnectionSecrets(state));
  for (const connection of state.connections) {
    if (!connection.connectionString) {
      await storage.secrets.delete(secretKey(connection.id));
    }
  }
  for (const connection of previous?.connections ?? []) {
    if (!state.connections.some(current => current.id === connection.id)) {
      await storage.secrets.delete(secretKey(connection.id));
    }
  }
}
