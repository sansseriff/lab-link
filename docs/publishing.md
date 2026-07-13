# Publishing

Publish the backend and frontend separately:

- PyPI package: `lab-link`, from `python/`
- npm package: `lab-link`, from `js/`

Keep the versions aligned. A protocol-level breaking change should bump both
packages, even if most code changed on only one side. This is simpler for app
authors than reasoning about a compatibility matrix.

## GitHub Actions Publishing

The repository includes two trusted-publishing workflows:

- `.github/workflows/publish-python.yml`
- `.github/workflows/publish-npm.yml`

Both run on `v*` tags and via manual dispatch. Both verify that
`python/pyproject.toml` and `js/package.json` have the same version. For tag
builds, they also require the tag to be exactly `v` plus that package version.

### PyPI Trusted Publisher

On PyPI, configure a GitHub Actions trusted publisher for:

- Owner: `sansseriff`
- Repository: `lab-link`
- Workflow filename: `publish-python.yml`
- Environment name: `pypi`

The workflow builds from `python/` and uploads `python/dist` with
`pypa/gh-action-pypi-publish`.

### npm Trusted Publisher

On npm, configure a trusted publisher for the JavaScript package:

- Organization or user: `sansseriff`
- Repository: `lab-link`
- Workflow filename: `publish-npm.yml`
- Environment name: `npm`
- Allowed action: `npm publish`

The npm workflow uses GitHub OIDC, Node 24, npm publish, and Bun for install,
test, and build.

If the package is scoped, change `js/package.json` to the scoped name and update
the publish command to:

```bash
npm publish --access public
```

## Manual Release Flow

1. Update `python/pyproject.toml` and `js/package.json` to the same version.
2. Run tests and builds.
3. Commit the version change.
4. Tag that commit, for example `v0.5.0`.
5. Push the branch and tag. The tag triggers both publish workflows.

```bash
git push origin master
git tag v0.5.0
git push origin v0.5.0
```

If the unscoped npm name is unavailable, use a scoped package such as
`@sansseriff/lab-link`. Keep the PyPI package name as `lab-link` unless there is
a concrete reason to rename it.

## Why Two Packages?

Python users should not install browser build artifacts to control instruments,
and frontend users should not install Starlette or Pydantic. Publishing separate
packages keeps dependency graphs clean while a shared repository keeps the
protocol, docs, examples, and release versions coordinated.
