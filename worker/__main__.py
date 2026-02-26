"""Allow running as `python -m worker`."""
import asyncio

from .worker import main

asyncio.run(main())
