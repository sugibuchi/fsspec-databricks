import logging
from logging import Logger
from typing import Any

from aiohttp import (
    ClientSession,
)


class AioHttpClientMixin:
    """A mixin class for using aiohttp HTTP client"""

    log: Logger = logging.getLogger(__name__)
    """Logger object."""

    __session: ClientSession | None = None
    """The aiohttp `ClientSession` object."""

    __session_params: dict[str, Any] | None = None
    """Keyword arguments to pass to the aiohttp `ClientSession` constructor."""

    def _config_session(self, **kwargs) -> None:
        """Configure the aiohttp `ClientSession` object.

        Parameters
        ----------
        **kwargs
            Keyword arguments to pass to the aiohttp `ClientSession` constructor.
        """
        self.__session_params = dict(kwargs)

    @property
    def _session(self) -> ClientSession:
        """Get an aiohttp `ClientSession` object.

        This session object must be accessed and used in the event loop that was passed to the constructor.
        """
        if self.__session is None:
            self.log.debug("Creating new aiohttp ClientSession")

            if self.__session_params is None:
                self.__session_params = {}

            self.__session = ClientSession(**self.__session_params)

        return self.__session

    async def _close_session(self) -> None:
        """Close the aiohttp `ClientSession` object if it exists."""
        try:
            if self.__session is not None:
                self.log.debug("Closing aiohttp ClientSession")
                await self.__session.close()
                self.log.debug("Closed aiohttp ClientSession")
        finally:
            self.__session = None


__all__ = [
    "AioHttpClientMixin",
]
