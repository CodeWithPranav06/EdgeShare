# Team workflow

1. **Never push to `main`.** All work goes through a pull request (PR).
2. **One branch per feature**, named `feat/<area>-<short-name>`, e.g. `feat/networking-heartbeat`.
3. **Edit only your own folders.** Shared files (`config.py`, `INTERFACES.md`) change only through a PR that member A approves.
4. **Small PRs** (one feature each), reviewed by a *different* teammate before merging.
5. **Run `python -m pytest` before opening a PR.** It must pass.
6. Commit messages: short and specific, e.g. `Add heartbeat timeout to recovery`.

## Daily commands

```bash
git checkout main && git pull                # start of the day
git checkout -b feat/<area>-<name>           # new feature branch
git add <files> && git commit -m "message"   # save work
git push -u origin feat/<area>-<name>        # first push of the branch
# open a Pull Request on GitHub, get a review, then "Squash and merge"
git checkout main && git pull                # after merging
```

If your branch is behind `main`: `git checkout main && git pull && git checkout - && git merge main`, fix any conflicts, run the tests, push.

## Before the demo

Do not merge anything risky in the last 48 hours. Tag the working version: `git tag v1.0 && git push --tags`.
