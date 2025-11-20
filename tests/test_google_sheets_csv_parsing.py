from src.bot.commands.groups import _parse_rows_to_groups


def test_parse_rows_to_groups_with_header_and_data():
    rows = [
        ["Название", "Username", "Ссылка"],
        ["Group One", "@group_one", ""],
        ["", "group_two", "https://t.me/group_two"],
        ["   ", "", "t.me/group_three"],
        ["", "", ""],
    ]
    parsed = _parse_rows_to_groups(rows)
    # Should skip header and empty row, keep 3 rows
    assert len(parsed) == 3
    # Normalization: username '@' removed, links normalized
    assert parsed[0].username == "group_one"
    assert parsed[1].link.endswith("/group_two")
    assert parsed[2].link.endswith("/group_three")


def test_parse_rows_to_groups_empty_table():
    rows = [[]]
    parsed = _parse_rows_to_groups(rows)
    assert parsed == []
