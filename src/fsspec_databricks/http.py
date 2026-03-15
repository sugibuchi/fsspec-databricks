import logging
from logging import Logger

from aiohttp import (
    ClientSession,
)


class AioHttpClientMixin:
    """A mixin class for using aiohttp HTTP client"""

    log: Logger = logging.getLogger(__name__)
    """Logger object."""

    __session: ClientSession | None = None
    """The aiohttp `ClientSession` object."""

    __session_params: dict
    """Keyword arguments to pass to the aiohttp `ClientSession` constructor."""

    def _config_session(self, **kwargs):
        """Configure the aiohttp `ClientSession` object.

        Parameters
        ----------
        **kwargs
            Keyword arguments to pass to the aiohttp `ClientSession` constructor.
        """
        self.__session_params = dict(kwargs)

    @property
    def _session(self):
        """Get an aiohttp `ClientSession` object.

        This session object must be accessed and used in the event loop that was passed to the constructor.
        """
        if self.__session is None:
            self.log.debug("Creating new aiohttp ClientSession")

            self.__session = ClientSession(**self.__session_params)

        return self.__session

    async def _close_session(self):
        """Close the aiohttp `ClientSession` object if it exists."""
        try:
            if self.__session is not None:
                self.log.debug("Closing aiohttp ClientSession")
                await self.__session.close()
        finally:
            self.__session = None


__all__ = [
    "AioHttpClientMixin",
]
