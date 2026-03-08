const test = require('node:test');
const assert = require('node:assert/strict');

const {
  getMappingBaselineDefaults,
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
  assert.equal(profile.modelFileNamePattern, '{NamePascal}Model.cs');
  assert.equal(profile.reviewCadence, 'quarterly');
  assert.equal(profile.nextPlannedReviewOn, '2026-06-30');
  assert.equal(profile.recommendedTestFocus, 'Consistency-oriented tests for batch flows and DML validation.');
  assert.equal(profile.mappingDefaults.Table.targetFolder, 'tests/Consistency/Tables');
  assert.equal(profile.mappingDefaults.Procedure.fileSuffix, 'ProcedureConsistencyTests');
  assert.equal(profile.mappingDefaults.Sequence.targetFolder, 'tests/Consistency/Sequences');
});

test('getMappingBaselineDefaults resolves api recommendations for test mappings', () => {
  const defaults = getMappingBaselineDefaults('api');

  assert.equal(defaults.Table.targetFolder, 'tests/Integration/Tables');
  assert.equal(defaults.View.fileSuffix, 'ViewIntegrationTests');
  assert.equal(defaults.Procedure.targetFolder, 'tests/Integration/Procedures');
  assert.equal(defaults.Sequence.fileSuffix, 'SequenceIntegrationTests');
});

test('resolveTemplateSettingsDefaults falls back to current api baseline', () => {
  const settings = resolveTemplateSettingsDefaults({
    modelTemplatePath: '',
    repositoryTemplatePath: '',
    modelTargetFolder: '',
    repositoryTargetFolder: '',
    modelFileNamePattern: '',
    repositoryFileNamePattern: ''
  });

  assert.equal(settings.modelTemplatePath, 'templates/dbsqllikemem/vCurrent/api/model.template.txt');
  assert.equal(settings.repositoryTemplatePath, 'templates/dbsqllikemem/vCurrent/api/repository.template.txt');
  assert.equal(settings.modelTargetFolder, 'src/Models');
  assert.equal(settings.repositoryTargetFolder, 'src/Repositories');
  assert.equal(settings.modelFileNamePattern, '{NamePascal}Model.cs');
  assert.equal(settings.repositoryFileNamePattern, '{NamePascal}Repository.cs');
});
