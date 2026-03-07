const test = require('node:test');
const assert = require('node:assert/strict');

const {
  getTemplateBaselineProfile,
  getTemplateBaselineProfiles,
  resolveTemplateSettingsDefaults
} = require('../out/template-baselines.js');

test('getTemplateBaselineProfiles exposes api and worker entries', () => {
  const profiles = getTemplateBaselineProfiles();

  assert.equal(profiles.length, 2);
  assert.equal(profiles[0].id, 'api');
  assert.equal(profiles[1].id, 'worker');
});

test('getTemplateBaselineProfile resolves worker baseline', () => {
  const profile = getTemplateBaselineProfile('worker');

  assert.equal(profile.id, 'worker');
  assert.equal(profile.modelTemplatePath, 'templates/dbsqllikemem/vCurrent/worker/model.template.txt');
  assert.equal(profile.repositoryTargetFolder, 'src/Batch/Repositories');
});

test('resolveTemplateSettingsDefaults falls back to current api baseline', () => {
  const settings = resolveTemplateSettingsDefaults({
    modelTemplatePath: '',
    repositoryTemplatePath: '',
    modelTargetFolder: '',
    repositoryTargetFolder: ''
  });

  assert.equal(settings.modelTemplatePath, 'templates/dbsqllikemem/vCurrent/api/model.template.txt');
  assert.equal(settings.repositoryTemplatePath, 'templates/dbsqllikemem/vCurrent/api/repository.template.txt');
  assert.equal(settings.modelTargetFolder, 'src/Models');
  assert.equal(settings.repositoryTargetFolder, 'src/Repositories');
});
