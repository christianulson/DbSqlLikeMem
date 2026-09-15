export interface SqlServerConnectionStringParts {
  server?: string;
  database?: string;
  userId?: string;
  password?: string;
}

export function parseSqlServerConnectionString(connectionString: string): SqlServerConnectionStringParts {
  const map = new Map<string, string>();
  for (const pair of splitConnectionStringPairs(connectionString)) {
    const separatorIndex = pair.indexOf('=');
    if (separatorIndex <= 0) {
      continue;
    }

    const key = pair.slice(0, separatorIndex).trim().toLowerCase();
    const value = unquoteConnectionStringValue(pair.slice(separatorIndex + 1).trim());
    if (key) {
      map.set(key, value);
    }
  }

  return {
    server: map.get('server') ?? map.get('data source'),
    database: map.get('database') ?? map.get('initial catalog'),
    userId: map.get('user id') ?? map.get('uid'),
    password: map.get('password') ?? map.get('pwd')
  };
}

export function splitConnectionStringPairs(connectionString: string): string[] {
  const pairs: string[] = [];
  let current = '';
  let quote: string | undefined;
  let braceDepth = 0;

  for (const ch of connectionString) {
    if (quote) {
      current += ch;
      if (ch === quote) {
        quote = undefined;
      }
      continue;
    }

    if (ch === '"' || ch === '\'') {
      quote = ch;
      current += ch;
      continue;
    }

    if (ch === '{') {
      braceDepth += 1;
      current += ch;
      continue;
    }

    if (ch === '}' && braceDepth > 0) {
      braceDepth -= 1;
      current += ch;
      continue;
    }

    if (ch === ';' && braceDepth === 0) {
      if (current.trim()) {
        pairs.push(current);
      }
      current = '';
      continue;
    }

    current += ch;
  }

  if (current.trim()) {
    pairs.push(current);
  }

  return pairs;
}

export function unquoteConnectionStringValue(value: string): string {
  if (value.length >= 2
    && ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith('\'') && value.endsWith('\'')))) {
    return value.slice(1, -1);
  }

  return value;
}
