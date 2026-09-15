export interface ManagerMapping {
  objectType: string;
  targetFolder: string;
  fileSuffix: string;
  namespace?: string;
}

export interface ManagerStateSource {
  connections: ReadonlyArray<{ id: string; name: string; databaseType: string; databaseName: string; connectionString: string }>;
  mappingConfigurations: ReadonlyArray<{ connectionId: string; mappings: ManagerMapping[] }>;
}

// Send only the fields the UI needs; credentials never enter the webview.
export function createManagerViewState(state: ManagerStateSource) {
  return {
    connections: state.connections.map(({ id, name, databaseType, databaseName, connectionString }) => ({
      id, name, databaseType, databaseName, hasCredentials: !!connectionString
    })),
    mappingConfigurations: state.mappingConfigurations.map(({ connectionId, mappings }) => ({
      connectionId,
      mappings: mappings.map(({ objectType, targetFolder, fileSuffix, namespace }) => ({
        objectType, targetFolder, fileSuffix, namespace: namespace ?? ''
      }))
    }))
  };
}

export function serializeWebviewData(value: unknown): string {
  return JSON.stringify(value).replace(/</g, '\\u003c');
}

export function readMappingNamespace(payload: Record<string, unknown>, objectType: string): string | undefined {
  const key = objectType.toLowerCase() + 'Namespace';
  return String(payload[key] ?? payload.namespace ?? '').trim() || undefined;
}
