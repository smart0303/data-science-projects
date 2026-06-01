from etl.transform import clean_and_transform


def test_clean_and_transform_normalizes_records():
    raw_records = [
        {"name": "  alice  ", "sales": "100"},
        {"name": "BOB", "sales": 250},
    ]

    result = clean_and_transform(raw_records)

    assert result == [
        {"name": "Alice", "sales": 100},
        {"name": "Bob", "sales": 250},
    ]
