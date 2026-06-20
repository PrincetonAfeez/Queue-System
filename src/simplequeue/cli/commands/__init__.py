"""CLI subcommands.

Each module exposes ``register(subparsers, parents)`` to add its argparse
subparser and a ``run(args, config) -> int`` handler. ``cli.main`` wires them
together; every handler is a thin shell over the ``Queue`` API.
"""
