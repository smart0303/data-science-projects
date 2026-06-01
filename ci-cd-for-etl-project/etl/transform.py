"""Simple transformation logic used by tests."""


def clean_and_transform(records: list[dict]) -> list[dict]:
    """Normalize records by trimming names and coercing sales to int."""
    transformed = []
    for record in records:
        name = str(record.get("name", "")).strip().title()
        sales = int(record.get("sales", 0))
        transformed.append({"name": name, "sales": sales})

    return transformed
