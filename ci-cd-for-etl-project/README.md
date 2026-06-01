# CI/CD for ETL Project

This project includes a GitHub Actions pipeline for ETL quality checks.

## Pipeline Name

`ETL-CI-Pipeline`

## Included Steps

- Python environment setup
- Dependency installation
- Automated tests with `pytest`
- Linting with `ruff`

## Trigger

The workflow runs on every push and pull request to `main`.

## Step 1: Push project to GitHub

If this repository is already connected to GitHub, run:

```bash
git add ci-cd-for-etl-project
git commit -m "Add ETL CI pipeline with tests and linting"
git push origin main
```
