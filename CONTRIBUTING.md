# Contributing

Thanks for looking into making `dbt-coves` better! We have some loosely defined rules and preferences to make the contribution a bit smoother. Don't let those deter you from contributing though, most of them can absolutely be fixed in the PR process.

## How can you contribute?

- Contributing **usually starts with creating an issue** (reporting a bug, or discussing a feature). In fact, even if you don't know how to code it or don't have the time, you're already helping out by pointing out potential issues or functionality that you would like to see implemented.
- See an already published issue that you think you can tackle? Create an issue for it and ask questions on how your idea and how you can help, it's generally a good way to make sure you'll hit the ground running quickly. It's not fun to do a lot of work and then find out the proposed change doesn't fit with the maintainer's goals.

## Advised Process/Conventions

### Git stuff

It would be really nice if you could follow a few of the guidelines around branch and PR naming, but don't let that deter you from contributing (as we said earlier these will be fixable during the PR process --but it may be annoying to you to get comments that could sound like nitpicking).

#### Branches

Below is the preferred format:

```bash
feat/<issue_number_if_applicable>/my_awesome_feature
fix/<issue_number_if_applicable>/describe_what_you_fix
refactor/<issue_number_if_applicable>/describe_what_you_refactor
docs/<issue_number_if_applicable>/what_is_changing_what_you_are_explaining
```

#### Pull Requests

We follow [conventional-commits](https://www.conventionalcommits.org/en/v1.0.0/) to standardize commits, and help with CHANGELOG.md generation.
If you feel like you don't want to bother with it it's ok, we squash all commits and we ensure the PR is named with a [conventional commit](https://www.conventionalcommits.org/en/v1.0.0/) pattern when we merge your branch.

**PR linting will fail if the PR isn't name with one of prefixes below**. Don't worry though, we'll get that sorted before merging 🎉

Here is what your PR names should ideally look like:

```txt
feat: my awesome feature
fix: relax version requirements on pandas
docs: document new feature
refactor: move cleanups into its own class
```

Use the template provided for you as a guide to cover most aspects of your PR. Feel free to delete stuff you think isn't relevant. The template is **just** a guide.

#### Commits and Conventions

Please follow generally accepted git best practices by using:

- **descriptive commit messages**,
- **imperative mood**

> **TIP**: ✨ it generally helps to say in your head "This commit will..." and then start writing. You'll end up with the right phrasing and it will trigger you to think about a useful description.

This leads to you writing commit messages such as `implement my awesome feature` instead of `implementing feature a` or `implemented feature a` which are less conventional.

We'll squash your PR at merge time.

#### Shout out your hard work in the changelog!

In this project we use `towncrier` to generate our changelog. When you make a PR, no-one better than you should be able to describe your code change! Towncrier works with news fragments. For each PR that proposes changes that need to be talked about in the changelog you should create a news fragment file in the following way:

1. `cd changelog/`
2. using your editor of choice create a file following the format below. Make sure to pick the category (feature, fix or misc) wisely:
   `<issue_or_pr_number>.<feature/fix/misc>.md`
3. save it and you're done. The release manager will take care of aggregating the changelog at release time

#### Fork it, let's go! 🥁

Fork the repo and get going.
Here's a quick guide:

**Shit how do we do this fork thing?**

For official guidelines check the [GitHub documentation](https://docs.github.com/en/free-pro-team@latest/github/getting-started-with-github/fork-a-repo)

1. Create a fork from this repo in your accout by clicking the "Fork" button
2. Clone the fork on your machine via ssh or http depending on how you like to authenticate. For example:

   ```bash
   git clone git@github.com:<your_username>/dbt-coves.git
   ```

3. Modify your code locally.

   As you would for a regular cloned repo, `git branch`, `git add`, `git commit`, `git push`.
   The push will go to your fork remote which is totally fine.

4. Create a Pull Request from your own forked repository.

   When the time comes to merge your code back on the [original repo](https://github.com/datacoves/dbt-coves). Go to your fork's GitHub page, click the "Pull Request" button and choose to merge into the original repo.

5. [Keep your fork up to date](https://docs.github.com/en/free-pro-team@latest/github/getting-started-with-github/fork-a-repo#keep-your-fork-synced). This is required if you want to integrate new features from master into your branch or if you want to resolve upstream conflicts. Don't worry too much about it the maintainers will help you with this if you don't really feel 100%.

   Say, you want to update or pull the original repo you can add the original repo as another remote called `upstream` (actually you can name if whatever you want but usually that's how people name this) to your config like so:

   ```bash
   git remote add upstream https://github.com/datacoves/dbt-coves.git
   ```

### Python Stuff

#### Pre-commit

We have configured [`pre-commit`](https://pre-commit.com/) in our repository to ensure that all things related to linting, code formatting, and import sorting is taken care of ahead of the PR. We want to make sure the review process can focus on the code change that matter rather than petty discussion about line-length, indentation etc.

It is recommended to install `pre-commit` on your machine if you have not done so already by doing the following:

```bash
pip install pre-commit
```

If you don't want to bother, that's also OK because we also have [pre-commit.ci](https://pre-commit.ci/) on the PR side of things, you might just get annoying test failures that you could have taken care of earlier. But no worries, we'll help you out!

#### Formatting (Enforced by pre-commit)

- We format our code with the [`black`](https://github.com/psf/black) formatter. The pre-commit hooks will attempt to fix your formatting issues for you but it's "annoying" so you can set up `black` to format your code on save in your IDE and then you'll never have a problem again. The good thing with black is that we don't ever have to discuss formatting and style while reviewing your PR which is soooooo much nicer! :heart_eyes:

- [`flake8`](https://flake8.pycqa.org/en/latest/) is also part of our pre-commit config. If you do not have it installed as a linter in your IDE or you ignore its recommendation `pre-commit` will fail. **flake8 in precommits does not automatically fix your files** so it's better to enforce it's advice during developments but that's up to you.

- Finally, **trailing whitespace** and **empty new line at end of file** will also be enforced for you by `pre-commit` either during PR time or locally if you have set it up according to [our guidelines](#pre-commit).

#### Type Hinting

- It is recommended to use Type Hinting and have [`mypy`](http://mypy-lang.org/) enabled as your linter. Most IDE's have an extension or a way to help with this. Typing isn't necessary but **really, really** preferred. The maintainers might therefore make suggestions on how to implement typing or will enforce it for you directly in your branch.
- Mypy is also part of our `pre-commit` and it should alert you if you have any issues with type hints.

### Development

The whole package is managed using [Poetry](https://python-poetry.org/). It's really really good and ensures reproducibility. \*\*If this is holding you up from contributing feel free to shoot us a DM on [Discord](https://discord.gg/bUk4MVTcqW) and we can see what are other ways. It's likely that `pip` would understand the `pyproject.toml` file and then you'll be free to set up a virtual environment of your choice and get going.

#### Installing Poetry

1. [Install Poetry on your system](https://python-poetry.org/docs/#installation) --I personally recommend installing it via [`pipx`](https://github.com/pipxproject/pipx) but that's entirely up to you.
2. `cd` to your fork and run `poetry install` in the root folder of the repo. Poetry will create a virtual environment for python, install dependencies and install `dbt-coves` in **editable** mode so that you can directly run and test `dbt-cove` as you develop without having to install over and over.
3. The easiest way to activate the poetry virtual env is to call `poetry shell` it'll wrap around your shell and activate the dbt-cove venv, to deactivate or get out of the shell simply type `exit`. More info on [the Poetry documentation website](https://python-poetry.org/docs/basic-usage/#using-your-virtual-environment)

### Testing

If you're comfortable writing tests for your features, we use the [`pytest`](https://docs.pytest.org/en/stable/) framework. It'll be installed automatically when you set up your poetry environment. If you don't know how to write tests that's fine, we'll work it out with you during the review process 💪🏻

For our current tests, we must have certain environment variables defined in a `.env` file inside `/tests`:

```
PROFILE_DBT_COVES_REDSHIFT
HOST_REDSHIFT
USER_REDSHIFT
PASSWORD_REDSHIFT
DATABASE_REDSHIFT
SCHEMA_REDSHIFT
PROFILE_DBT_COVES_SNOWFLAKE
USER_SNOWFLAKE
PASSWORD_SNOWFLAKE
ACCOUNT_SNOWFLAKE
WAREHOUSE_SNOWFLAKE
ROLE_SNOWFLAKE
DATABASE_SNOWFLAKE
SCHEMA_SNOWFLAKE
PROFILE_DBT_COVES_BIGQUERY
PROJECT_BIGQUERY
DATASET_BIGQUERY
SERVICE_ACCOUNT_GCP
```

We recommend using 1Password. In it, an entry named `dbt-coves-tests` with the keys above, installing [1Password CLI](https://developer.1password.com/docs/cli/) and running `tests/generate_onepsw_env_file.py`

Once you have the `.env` file ready, just run:

```bash
pytests tests/
```
