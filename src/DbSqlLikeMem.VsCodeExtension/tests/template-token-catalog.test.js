const test = require('node:test');
const assert = require('node:assert/strict');

const {
  findUnsupportedTemplateTokens,
  getSupportedTemplateTokens
} = require('../out/template-token-catalog.js');

test('getSupportedTemplateTokens exposes documented generation placeholders', () => {
  const tokens = getSupportedTemplateTokens();

  assert.deepEqual(tokens, [
    '{{ClassName}}',
    '{{ObjectName}}',
    '{{Schema}}',
    '{{ObjectType}}',
    '{{DatabaseType}}',
    '{{DatabaseName}}',
    '{{Namespace}}'
  ]);
});

test('findUnsupportedTemplateTokens returns only unknown placeholders', () => {
  const tokens = findUnsupportedTemplateTokens(`
    {{ClassName}}
    {{Schema}}
    {{UnknownToken}}
    {{AnotherUnknown}}
  `);

  assert.deepEqual(tokens, ['{{AnotherUnknown}}', '{{UnknownToken}}']);
});

test('findUnsupportedTemplateTokens ignores documented placeholders', () => {
  const tokens = findUnsupportedTemplateTokens(`
    {{ClassName}}
    {{ObjectName}}
    {{Schema}}
    {{ObjectType}}
    {{DatabaseType}}
    {{DatabaseName}}
    {{Namespace}}
  `);

  assert.deepEqual(tokens, []);
});
