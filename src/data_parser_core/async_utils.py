"""Async utility functions for the data parser service.

This module contains utility functions for handling async operations,
particularly for stream processing and data conversion.
"""

from collections.abc import AsyncGenerator


async def async_bytes_to_text_stream(
    async_bytes_stream: AsyncGenerator[bytes, None], 
    encoding: str = "utf-8"
) -> AsyncGenerator[str, None]:
    """Convert async bytes stream to async text stream.
    
    Args:
        async_bytes_stream: Async generator yielding bytes chunks
        encoding: Text encoding to use (default: utf-8)
        
    Yields:
        str: Text lines from the stream
    """
    buffer = b""
    
    async for chunk in async_bytes_stream:
        buffer += chunk
        
        # Try to decode complete lines
        while b'\n' in buffer:
            line, buffer = buffer.split(b'\n', 1)
            try:
                yield line.decode(encoding)
            except UnicodeDecodeError:
                # Handle encoding errors gracefully
                yield line.decode(encoding, errors='replace')
    
    # Yield any remaining data
    if buffer:
        try:
            yield buffer.decode(encoding)
        except UnicodeDecodeError:
            yield buffer.decode(encoding, errors='replace')
