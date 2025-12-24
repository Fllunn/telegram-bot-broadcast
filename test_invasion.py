from src.services.auto_invasion.link_parser import parse_group_link

test_links = [
    "https://t.me/testgroup",
    "https://t.me/+AAAAAAAAAAAA",
    "https://t.me/joinchat/AAAAAAAAAAAA",
    "http://t.me/group",
    "invalid_link",
]

for link in test_links:
    result = parse_group_link(link)
    print(f"Link: {link}")
    print(f"Parsed: {result}\n")
