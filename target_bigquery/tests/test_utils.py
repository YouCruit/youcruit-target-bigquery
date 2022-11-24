import os
from io import StringIO


def get_test_tap_lines(filename: str) -> StringIO:
    """Returns an IO-stream with some singer tap output"""
    stream = StringIO()

    resources_path = os.path.join(os.path.dirname(__file__), "resources")
    filepath = os.path.join(resources_path, filename)
    with open(filepath) as tap_stdout:
        for line in tap_stdout.readlines():
            print(
                line.replace("PATHGOESHERE", resources_path),
                end="",
                file=stream,
            )

    stream.seek(0)
    return stream
