"""Parser execution engine for processing data bundles.

This module contains the core parser logic that orchestrates the data processing
from raw stage to parsed stage using the pipeline bus and parser strategies.
"""

import asyncio
import hashlib
import io
import os
import re
from datetime import UTC, datetime
from typing import Any

import structlog

from data_parser_app.app_config import parserConfig

from data_parser_core.async_utils import async_bytes_to_text_stream
from data_parser_core.core import DataRegistryParserConfig
from data_parser_core.exceptions import BundleError, ConfigurationError
from data_parser_core.jsonl_stream import JSONLStream
from oc_pipeline_bus import DataPipelineBus

# Get logger for this module
logger = structlog.get_logger(__name__)


async def run_parser(
    app_config: parserConfig,
    parser_config: DataRegistryParserConfig,
    data_registry_id: str,
    stage: str,
) -> None:
    """Run the async parser to process bundles from raw stage to parsed stage.

    Args:
        app_config: Application configuration with storage and credentials.
        parser_config: Parser configuration with resource parsers.
        data_registry_id: The data registry ID being processed.
        stage: The current stage (should be "raw" for parser).
    """

    logger.info(
        "PARSER_STARTING",
        data_registry_id=data_registry_id,
        stage=stage,
        concurrency=parser_config.concurrency,
    )

    data_pipeline_bus = DataPipelineBus()

    try:
        # Get change event and bundle info
        change_event = data_pipeline_bus.get_change_event()
        last_stage = change_event.stage
        bid = change_event.bid

        bundle_metadata = data_pipeline_bus.get_bundle_metadata_json(bid, last_stage)
        logger.info(
            "BUNDLE_FOUND", bid=bid, stage=last_stage, bundle_metadata=bundle_metadata
        )

        # Get list of resources
        resources = data_pipeline_bus.get_bundle_resource_list(bid)
        logger.info("BUNDLE_RESOURCES", bid=bid, resources=resources)

        if not resources:
            raise BundleError("No resources to process")

        # Create work queue
        work_queue = asyncio.Queue()
        for resource_name in resources:
            await work_queue.put(
                {
                    "resource_name": resource_name,
                    "bid": bid,
                    "parser_config": parser_config,
                }
            )

        # Create worker tasks
        workers = [
            asyncio.create_task(process_resource_worker(work_queue, data_pipeline_bus))
            for _ in range(parser_config.concurrency)
        ]

        # Wait for all work to complete
        await work_queue.join()

        # Wait for workers to finish naturally
        await asyncio.gather(*workers, return_exceptions=True)

        # Complete the bundle
        data_pipeline_bus.complete_bundle(
            bid,
            {
                "completed_at": datetime.now(UTC).isoformat(),
            },
        )

        logger.info("PARSER_COMPLETED", bid=bid, stage=stage)

    except Exception as e:
        logger.exception(
            "PARSER_ERROR", data_registry_id=data_registry_id, error=str(e)
        )
        raise  # Fail fast as requested


async def process_resource_worker(
    work_queue: asyncio.Queue, data_pipeline_bus: DataPipelineBus
) -> None:
    """Worker that processes resources from the queue with streaming."""

    while True:
        try:
            # Get work item
            work_item = await work_queue.get()
            resource_name = work_item["resource_name"]
            bid = work_item["bid"]
            parser_config = work_item["parser_config"]

            # Find parser strategy
            parser_strategy = None

            for pattern, parser_config_dict in parser_config.resource_parsers.items():
                if re.match(pattern, resource_name):
                    # Get the parser strategy (first key in the dict)
                    parser_strategy = list(parser_config_dict.keys())[0]
                    break

            if not parser_strategy:
                raise ConfigurationError(
                    f"No parser found for resource: {resource_name}"
                )

            # Create streaming pipeline
            async def create_output_stream():
                """Convert text stream to bytes for pipeline bus."""
                output_buffer = io.StringIO()
                jsonl_stream = JSONLStream(output_buffer)

                # Get async input stream from pipeline bus
                async_bytes_stream = data_pipeline_bus.get_bundle_resource_stream(bid, resource_name)
                
                # Convert async bytes stream to async text stream
                async_text_stream = async_bytes_to_text_stream(async_bytes_stream)

                # Stream parse with progress updates
                async for records_written in parser_strategy.parse_stream(
                    async_text_stream, jsonl_stream
                ):
                    logger.info(
                        "PROGRESS", resource=resource_name, records=records_written
                    )

                # Convert final output to bytes
                jsonl_content = output_buffer.getvalue()
                yield jsonl_content.encode("utf-8")

            # Stream directly to pipeline bus
            await data_pipeline_bus.add_bundle_resource_streaming(
                bid,
                f"{resource_name}.jsonl",
                {
                    "source_resource": resource_name,
                    "parsed_at": datetime.now(UTC).isoformat(),
                },
                create_output_stream(),
                progress_callback=lambda bytes_uploaded: logger.info(
                    "UPLOAD_PROGRESS",
                    resource=resource_name,
                    bytes_uploaded=bytes_uploaded,
                ),
            )

            logger.info("RESOURCE_COMPLETED", resource=resource_name)

        except Exception as e:
            logger.exception("WORKER_ERROR", resource=resource_name, error=str(e))
            raise  # Fail fast - bubble up error
        finally:
            work_queue.task_done()
