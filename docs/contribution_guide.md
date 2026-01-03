# Contribution Guide

This is our contribution guide which is still under development.

- Create a new branch for everything you want to contribute and give it a meaningful name
    - E.g., use `feature/check-regex` as a branch for implementing a new `CheckRegex` check
- Add meaningful comments to your code
  - Each function should have a description
- Split complex code in separate functions
- Use PEP-8 style
- Use type hints if possible. They make life so much easier.
- If possible, write tests for your new code

We might not accept pull requests not complying to the rules mentioned above.

## Adding a Quality Check

### Implementing the Check

Take a look at the implementations of already existing checks and you will observe the following

- Checks are derived from the `CheckBase` class, which in turn is derived from the `Base` class
- Each check requires a `run` method
- Check arguments are given in the `__init__` method
- Use the caching functionality if applicable, e.g.
    - `df = main.cache.get_dataframe(...)` or
    - `main.cache.get_count(df)`
- A check always returns a `bool` value which is `True` in case the data passed the check successfully and `False`
  otherwise
- If a check is failing, add some informative data to the `failed_data` member of `BaseCheck`. Take a look at some
  implementations to see how we do this in our code
- Print and log some information for the user in case of success and failure

### Implementing Tests

- Implement some useful tests. There should be at least one test for a successful check and one test for a failing
  check.
- Think about corner cases that might require special attention
- If you need special data for your test, take a look at the test data generation in the `test` directory
    - You might be able to re-use some generated data need to write a data generation for your specific check

###  Updating the Documentation

- After implementing your check and tests, update the documentation accordingly

### Raise a Pull Request

- When you are done and sure everything is fine, you are free to raise a pull request
- Make suer that your code is up-to-date with the `development` branch. Try to solve any merge conflicts before raising the PR.
- Select any of the core team members as reviewers, we will review your pull request as soon as possible