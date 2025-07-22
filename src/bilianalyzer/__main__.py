import click

from .commands import auth_commands, fetch_commands, parse_commands, analyze_commands


@click.group()
def main():
    """BiliAnalyzer: Fetch and Analyze Bilibili Comments"""


main.add_command(auth_commands.auth)
main.add_command(fetch_commands.fetch)
main.add_command(parse_commands.parse)
main.add_command(analyze_commands.analyze)


if __name__ == "__main__":
    main()
