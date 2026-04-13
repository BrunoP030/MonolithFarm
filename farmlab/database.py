"""Wrapper de compatibilidade para a persistencia legada do dashboard.

A implementacao real do banco do dashboard fica em ``dashboard/database.py``.
"""

from dashboard.database import *  # noqa: F401,F403
from dashboard.database import main as _main


if __name__ == "__main__":
    _main()
