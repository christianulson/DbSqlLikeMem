const test = require('node:test');
const assert = require('node:assert/strict');

const {
  buildTemplateBaselineProfileSummary,
  getMappingBaselineDefaults,
  getTemplateBaselineProfile,
  getTemplateBaselineProfiles,
  parseTemplateReviewMetadata,
  validateTemplateBaselineProfileAlignment,
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

test('parseTemplateReviewMetadata reads governance fields from repository contract', () => {
  const metadata = parseTemplateReviewMetadata(`{
    "currentBaseline": "vCurrent",
    "promotionStagingPath": "templates/dbsqllikemem/vNext",
    "reviewCadence": "quarterly",
    "lastReviewedOn": "2026-03-08",
    "nextPlannedReviewOn": "2026-06-30",
    "profiles": {
      "api": { "focus": "Light integration tests for tables, views, and repositories." }
    },
    "evidenceFiles": ["CHANGELOG.md", "templates/dbsqllikemem/review-checklist.md"]
  }`);

  assert.equal(metadata.currentBaseline, 'vCurrent');
  assert.equal(metadata.reviewCadence, 'quarterly');
  assert.equal(metadata.lastReviewedOn, '2026-03-08');
  assert.equal(metadata.nextPlannedReviewOn, '2026-06-30');
  assert.equal(metadata.profileFocusById.api, 'Light integration tests for tables, views, and repositories.');
  assert.deepEqual(metadata.evidenceFiles, ['CHANGELOG.md', 'templates/dbsqllikemem/review-checklist.md']);
});

test('buildTemplateBaselineProfileSummary exposes review metadata and evidence count', () => {
  const profile = getTemplateBaselineProfile('api');
  const metadata = parseTemplateReviewMetadata(`{
    "currentBaseline": "vCurrent",
    "promotionStagingPath": "templates/dbsqllikemem/vNext",
    "reviewCadence": "quarterly",
    "lastReviewedOn": "2026-03-08",
    "nextPlannedReviewOn": "2026-06-30",
    "profiles": {
      "api": { "focus": "Light integration tests for tables, views, and repositories." }
    },
    "evidenceFiles": ["CHANGELOG.md", "docs/features-backlog/index.md"]
  }`);

  const summary = buildTemplateBaselineProfileSummary(profile, metadata);

  assert.match(summary, /API \(vCurrent\)/);
  assert.match(summary, /2026-03-08/);
  assert.match(summary, /2026-06-30/);
  assert.match(summary, /Outputs: src\/Models \| src\/Repositories\./);
  assert.match(summary, /Evidence files: 2\./);
});

test('buildTemplateBaselineProfileSummary exposes overdue review windows', () => {
  const profile = getTemplateBaselineProfile('worker');
  const metadata = parseTemplateReviewMetadata(`{
    "currentBaseline": "vCurrent",
    "promotionStagingPath": "templates/dbsqllikemem/vNext",
    "reviewCadence": "quarterly",
    "lastReviewedOn": "2025-12-31",
    "nextPlannedReviewOn": "2026-01-15",
    "profiles": {
      "worker": { "focus": "Consistency-oriented tests for batch flows and DML validation." }
    },
    "evidenceFiles": ["CHANGELOG.md"]
  }`);

  const summary = buildTemplateBaselineProfileSummary(profile, metadata, '2026-03-08');

  assert.match(summary, /Governance drift:/);
  assert.match(summary, /overdue/i);
});

test('validateTemplateBaselineProfileAlignment reports governance drift', () => {
  const profile = getTemplateBaselineProfile('worker');
  const warnings = validateTemplateBaselineProfileAlignment(profile, {
    currentBaseline: 'vNext',
    promotionStagingPath: 'templates/dbsqllikemem/vNext',
    reviewCadence: 'monthly',
    lastReviewedOn: '2026-03-08',
    nextPlannedReviewOn: '2026-05-01',
    profileFocusById: {
      worker: 'Different focus'
    },
    evidenceFiles: ['CHANGELOG.md']
  });

  assert.equal(warnings.length, 4);
  assert.ok(warnings.some((warning) => warning.includes('Current baseline')));
  assert.ok(warnings.some((warning) => warning.includes('Review cadence')));
  assert.ok(warnings.some((warning) => warning.includes('Next planned review')));
  assert.ok(warnings.some((warning) => warning.includes('Recommended focus')));
});

test('validateTemplateBaselineProfileAlignment reports overdue review windows', () => {
  const profile = getTemplateBaselineProfile('api');
  const warnings = validateTemplateBaselineProfileAlignment(profile, {
    currentBaseline: 'vCurrent',
    promotionStagingPath: 'templates/dbsqllikemem/vNext',
    reviewCadence: 'quarterly',
    lastReviewedOn: '2025-12-31',
    nextPlannedReviewOn: '2026-01-15',
    profileFocusById: {
      api: 'Light integration tests for tables, views, and repositories.'
    },
    evidenceFiles: ['CHANGELOG.md']
  }, '2026-03-08');

  assert.equal(warnings.length, 1);
  assert.ok(warnings.some((warning) => warning.includes('overdue')));
});
