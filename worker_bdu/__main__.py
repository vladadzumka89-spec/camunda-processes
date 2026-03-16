"""Allow running as `python -m worker_bdu`."""
import asyncio

from .worker import main

asyncio.run(main())
