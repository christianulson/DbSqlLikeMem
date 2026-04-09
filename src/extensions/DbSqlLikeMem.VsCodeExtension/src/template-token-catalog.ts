const TEMPLATE_TOKEN_REGEX = /\{\{[^{}\r\n]+\}\}/g;

const SUPPORTED_TEMPLATE_TOKENS = [
  '{{ClassName}}',
  '{{ObjectName}}',
  '{{Schema}}',
  '{{ObjectType}}',
  '{{DatabaseType}}',
  '{{DatabaseName}}',
  '{{Namespace}}'
] as const;

export function getSupportedTemplateTokens(): readonly string[] {
  return SUPPORTED_TEMPLATE_TOKENS;
}

export function findUnsupportedTemplateTokens(template: string): string[] {
  if (!template.trim()) {
    return [];
  }

  const supportedTokens = new Set(SUPPORTED_TEMPLATE_TOKENS.map((token) => token.toLowerCase()));
  const matches = template.match(TEMPLATE_TOKEN_REGEX) ?? [];
  return [...new Set(matches)]
    .filter((token) => !supportedTokens.has(token.toLowerCase()))
    .sort((left, right) => left.localeCompare(right));
}
