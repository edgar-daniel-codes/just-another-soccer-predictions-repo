
import os 
import argparse
from pathlib import Path


def safe_mkdir(path: Path) -> None:
    """
    Create a directory if it does not exist.

    :param path: Directory path.
    """
    path.mkdir(parents=True, exist_ok=True)
