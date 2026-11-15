# datasette-arrows

[![PyPI](https://img.shields.io/pypi/v/datasette-arrows.svg)](https://pypi.org/project/datasette-arrows/)
[![Changelog](https://img.shields.io/github/v/release/datasette/datasette-arrows?include_prereleases&label=changelog)](https://github.com/datasette/datasette-arrows/releases)
[![Tests](https://github.com/datasette/datasette-arrows/actions/workflows/test.yml/badge.svg)](https://github.com/datasette/datasette-arrows/actions/workflows/test.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/datasette/datasette-arrows/blob/main/LICENSE)

Streaming exports for Datasette using Arrow IPC.

## Installation

Install this plugin in the same environment as Datasette.
```bash
datasette install datasette-arrows
```
## Usage

Usage instructions go here.

## Development

To set up this plugin locally, first checkout the code. You can confirm it is available like this:
```bash
cd datasette-arrows
# Confirm the plugin is visible
uv run datasette plugins
```
To run the tests:
```bash
uv run pytest
```
