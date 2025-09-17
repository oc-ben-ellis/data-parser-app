"""Parser execution engine for processing data bundles.

This module contains the core parser logic that orchestrates the data processing
from raw stage to parsed stage using the pipeline bus and parser strategies.
"""

import io
import os
import re
from datetime import UTC, datetime
from typing import Any

import structlog

from data_parser_app.app_config import parserConfig
from data_parser_core.core import DataRegistryParserConfig

# Get logger for this module
logger = structlog.get_logger(__name__)


async def run_parser(
    app_config: parserConfig,
    parser_config: DataRegistryParserConfig,
    data_registry_id: str,
    stage: str,
) -> None:
    """Run the parser to process bundles from raw stage to parsed stage.
    
    Args:
        app_config: Application configuration with storage and credentials.
        parser_config: Parser configuration with resource parsers.
        data_registry_id: The data registry ID being processed.
        stage: The current stage (should be "raw" for parser).
    """
    from oc_pipeline_bus import DataPipelineBus
    
    logger.info(
        "PARSER_STARTING",
        data_registry_id=data_registry_id,
        stage=stage,
        concurrency=parser_config.concurrency,
    )
    
    # Initialize pipeline bus for raw stage (input) and parsed stage (output)
    input_bus = DataPipelineBus(
        bucket=os.environ.get("OC_DATA_PIPELINE_STORAGE_S3_URL", "oc-dev-data-pipeline"),
        stage="raw",
        data_registry_id=data_registry_id,
        orchestration_queue_url=os.environ.get("OC_DATA_PIPELINE_ORCHESTRATION_SQS_URL"),
    )
    
    output_bus = DataPipelineBus(
        bucket=os.environ.get("OC_DATA_PIPELINE_STORAGE_S3_URL", "oc-dev-data-pipeline"),
        stage="parsed",
        data_registry_id=data_registry_id,
        orchestration_queue_url=os.environ.get("OC_DATA_PIPELINE_ORCHESTRATION_SQS_URL"),
    )
    
    # Get the latest bundle from raw stage
    # For now, we'll process bundles that are ready for parsing
    # In a real implementation, this would be triggered by orchestration events
    
    # For testing purposes, let's process a specific bundle
    # In production, this would come from orchestration events
    test_bid = f"bid:v1:{data_registry_id}:20250911151245:ab38fe12"
    
    try:
        # Check if bundle exists in raw stage
        try:
            bundle_metadata = input_bus.get_bundle_metadata_json(test_bid)
            logger.info("BUNDLE_FOUND", bid=test_bid, metadata=bundle_metadata)
        except Exception as e:
            logger.warning("BUNDLE_NOT_FOUND", bid=test_bid, error=str(e))
            # For testing, create a mock bundle
            test_bid = input_bus.bundle_found({
                "source": "test",
                "discovered_at": datetime.now(UTC).isoformat(),
            })
            logger.info("MOCK_BUNDLE_CREATED", bid=test_bid)
        
        # Get list of resources in the bundle
        try:
            resources = input_bus.get_bundle_resource_list(test_bid)
            logger.info("BUNDLE_RESOURCES", bid=test_bid, resources=resources)
        except Exception as e:
            logger.warning("NO_RESOURCES_FOUND", bid=test_bid, error=str(e))
            resources = []
        
        if not resources:
            logger.info("NO_RESOURCES_TO_PROCESS", bid=test_bid)
            return
        
        # Create output bundle in parsed stage
        output_bid = output_bus.bundle_found({
            "source_bid": test_bid,
            "discovered_at": datetime.now(UTC).isoformat(),
            "stage": "parsed",
        })
        
        # Process each resource
        processed_resources = []
        for resource_name in resources:
            try:
                # Find matching parser for this resource
                parser_strategy = None
                parser_kv_args = None
                
                for pattern, parser_config_dict in parser_config.resource_parsers.items():
                    if re.match(pattern, resource_name):
                        # Get the parser strategy (first key in the dict)
                        parser_type = list(parser_config_dict.keys())[0]
                        parser_kv_args = parser_config_dict[parser_type]
                        
                        # Create parser instance
                        from data_parser_core.strategy_registration import create_strategy_registry
                        from data_parser_core.strategy_types import ResourceParserStrategy
                        registry = create_strategy_registry()
                        parser_strategy = registry.create(
                            ResourceParserStrategy,
                            parser_type,
                            parser_kv_args
                        )
                        break
                
                if not parser_strategy:
                    logger.warning("NO_PARSER_FOUND", resource_name=resource_name)
                    continue
                
                # Read the resource from input bundle
                resource_stream = input_bus.get_bundle_resource(test_bid, resource_name)
                resource_data = resource_stream.read()
                
                # Parse the resource
                output_filename = f"{resource_name}.jsonl"
                output_buffer = io.StringIO()
                
                # For testing, write to a temporary file first
                temp_filename = f"/tmp/{resource_name}"
                with open(temp_filename, "wb") as temp_file:
                    temp_file.write(resource_data)
                
                # Parse using the strategy
                records_written = parser_strategy.parse(
                    temp_filename,
                    output_buffer,
                    parser_kv_args
                )
                
                # Clean up temp file
                os.unlink(temp_filename)
                
                # Get the JSONL output
                jsonl_content = output_buffer.getvalue()
                
                # Add the parsed resource to output bundle
                output_bus.add_bundle_resource(
                    output_bid,
                    output_filename,
                    {
                        "source_resource": resource_name,
                        "source_bid": test_bid,
                        "records_written": records_written,
                        "parsed_at": datetime.now(UTC).isoformat(),
                    },
                    io.BytesIO(jsonl_content.encode('utf-8'))
                )
                
                processed_resources.append(output_filename)
                logger.info(
                    "RESOURCE_PARSED",
                    resource_name=resource_name,
                    output_filename=output_filename,
                    records_written=records_written,
                )
                
            except Exception as e:
                logger.exception(
                    "RESOURCE_PARSE_ERROR",
                    resource_name=resource_name,
                    error=str(e),
                )
                continue
        
        # Complete the output bundle
        if processed_resources:
            output_bus.complete_bundle(output_bid, {
                "source_bid": test_bid,
                "processed_resources": processed_resources,
                "completed_at": datetime.now(UTC).isoformat(),
            })
            
            logger.info(
                "PARSER_COMPLETED",
                input_bid=test_bid,
                output_bid=output_bid,
                processed_resources=processed_resources,
            )
        else:
            logger.warning("NO_RESOURCES_PROCESSED", input_bid=test_bid)
            
    except Exception as e:
        logger.exception(
            "PARSER_ERROR",
            data_registry_id=data_registry_id,
            error=str(e),
        )
        raise
