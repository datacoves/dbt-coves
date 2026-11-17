"""
Rewriting a properties file folded every value longer than 80 characters and left a
trailing space at each fold, so pre-commit trailing-whitespace hooks failed on files
dbt-coves had just updated.
"""

from dbt_coves.utils.yaml import open_yaml, save_yaml

LONG = "The month the borrower's earliest reported credit line was opened, in MMM-YYYY"

SOURCE = f"""version: 2
models:
  - name: stg_personal_loans
    columns:
      - name: earliest_cr_line
        description: {LONG}
      - name: collections_12_mths_ex_med
        description:
          Number of collections in the last 12 months excluding medical
          collections
"""


def test_rewrite_keeps_long_values_on_one_line(tmp_path):
    path = tmp_path / "props.yml"
    path.write_text(SOURCE)
    save_yaml(path, open_yaml(path))
    text = path.read_text()

    assert not [line for line in text.splitlines() if line != line.rstrip()]
    assert f"        description: {LONG}\n" in text
    columns = open_yaml(path)["models"][0]["columns"]
    assert columns[1]["description"] == (
        "Number of collections in the last 12 months excluding medical collections"
    )
