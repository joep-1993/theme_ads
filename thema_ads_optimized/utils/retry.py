"""Retry logic with exponential backoff."""

import asyncio
import logging
from functools import wraps
from typing import Callable, Any
from google.ads.googleads.errors import GoogleAdsException

logger = logging.getLogger(__name__)


def async_retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Decorator for async functions with exponential backoff retry."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            current_delay = delay

            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except GoogleAdsException as e:
                    last_exception = e

                    # Check if error is retryable
                    if hasattr(e, 'failure') and e.failure:
                        # Don't retry on certain errors (like invalid credentials, quota exceeded permanently)
                        error_codes = [err.error_code for err in e.failure.errors]
                        non_retryable = ['AUTHENTICATION_ERROR', 'AUTHORIZATION_ERROR', 'QUOTA_ERROR']

                        for error in e.failure.errors:
                            error_name = str(error.error_code).split('.')[0]
                            if error_name in non_retryable:
                                logger.error(f"Non-retryable error in {func.__name__}: {error_name}")
                                raise

                    if attempt < max_attempts:
                        logger.warning(
                            f"Attempt {attempt}/{max_attempts} failed for {func.__name__}. "
                            f"Retrying in {current_delay}s... Error: {str(e)[:100]}"
                        )
                        await asyncio.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
                        raise last_exception

                except Exception as e:
                    last_exception = e
                    logger.error(f"Unexpected error in {func.__name__}: {e}")
                    raise

            raise last_exception

        return wrapper
    return decorator


def sync_retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Decorator for sync functions with exponential backoff retry."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            import time
            last_exception = None
            current_delay = delay

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except GoogleAdsException as e:
                    last_exception = e

                    if attempt < max_attempts:
                        logger.warning(
                            f"Attempt {attempt}/{max_attempts} failed for {func.__name__}. "
                            f"Retrying in {current_delay}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
                        raise last_exception

                except Exception as e:
                    last_exception = e
                    logger.error(f"Unexpected error in {func.__name__}: {e}")
                    raise

            raise last_exception

        return wrapper
    return decorator
