"""
Temporal History simulation strategy implementation.
"""

from typing import Dict, List
import structlog
from typing import Any, Optional

from models.simulation_response import (
    SimulationStrategyOutput,
)
from strategies.base_strategy import BaseStrategy
from zamp_public_workflow_sdk.temporal.workflow_history.helpers import get_child_workflow_execution_info
from zamp_public_workflow_sdk.temporal.workflow_history.models import (
    WorkflowHistory,
    workflow_history,
)
from zamp_public_workflow_sdk.actions_hub import ActionsHub
from zamp_public_workflow_sdk.temporal.workflow_history.models.fetch_temporal_workflow_history import FetchTemporalWorkflowHistoryInput, FetchTemporalWorkflowHistoryOutput


logger = structlog.get_logger(__name__)

# Constants
MAIN_WORKFLOW_IDENTIFIER = "main_workflow"


class TemporalHistoryStrategyHandler(BaseStrategy):
    """
    Strategy that uses Temporal workflow history to mock node outputs.
    """

    def __init__(self, reference_workflow_id: str, reference_workflow_run_id: str):
        """
        Initialize with reference workflow details.

        Args:
            reference_workflow_id: Reference workflow ID to fetch history from
            reference_workflow_run_id: Reference run ID to fetch history from
        """
        self.reference_workflow_id = reference_workflow_id
        self.reference_workflow_run_id = reference_workflow_run_id

    async def execute(
        self,
        node_ids: List[str],
        temporal_history: Optional[WorkflowHistory] = None,
    ) -> SimulationStrategyOutput:
        """
        Execute Temporal History strategy.

        Args:
            node_ids: List of node execution IDs
            temporal_history: Optional workflow history (if already fetched)

        Returns:
            SimulationStrategyOutput with should_execute=False for mocking when history is found
        """
        try:
            if temporal_history is None:
                temporal_history = await self._fetch_temporal_history(node_ids)

            if temporal_history is not None:
                output = await self._extract_node_output(temporal_history, node_ids)
                if output is not None:
                    return SimulationStrategyOutput(
                        should_execute=False, node_outputs=output
                    )

            return SimulationStrategyOutput(should_execute=True, node_outputs={})

        except Exception as e:
            logger.error(
                "TemporalHistoryStrategyHandler: Error executing strategy",
                node_ids=node_ids,
                error=str(e),
                error_type=type(e).__name__,
            )
            return SimulationStrategyOutput(should_execute=True, node_outputs={})

    async def _fetch_temporal_history(
        self, node_ids: List[str], workflow_id: Optional[str] = None, run_id: Optional[str] = None
    ) -> Optional[WorkflowHistory]:
        """
        Fetch temporal workflow history for reference workflow or child workflow.

        Args:
            node_ids: List of node execution IDs
            workflow_id: Optional workflow ID to fetch history from (defaults to reference workflow)
            run_id: Optional run ID to fetch history from (defaults to reference run)

        Returns:
            WorkflowHistory object or None if fetch fails
        """
        try:
            # Use provided workflow_id and run_id, or fall back to reference workflow details
            target_workflow_id = workflow_id or self.reference_workflow_id
            target_run_id = run_id or self.reference_workflow_run_id
            
            workflow_history = await ActionsHub.execute_child_workflow(
                "FetchTemporalWorkflowHistoryWorkflow",
                FetchTemporalWorkflowHistoryInput(
                    workflow_id=target_workflow_id,
                    run_id=target_run_id,
                    node_ids=node_ids,
                ),
                result_type=FetchTemporalWorkflowHistoryOutput
            )
            return workflow_history

        except Exception as e:
            logger.error(
                "Failed to fetch temporal history",
                error=str(e),
                error_type=type(e).__name__,
                target_workflow_id=target_workflow_id,
                target_run_id=target_run_id,
                reference_workflow_id=self.reference_workflow_id,
                reference_workflow_run_id=self.reference_workflow_run_id,
            )
            return None

    async def _extract_node_output(
        self, temporal_history: WorkflowHistory, node_ids: List[str]
    ) -> Dict[str, Optional[Any]]:
        """
        Extract output for specific nodes from temporal history.

        Args:
            temporal_history: The workflow history object
            node_ids: List of node execution IDs to extract output for

        Returns:
            Dictionary mapping node IDs to their outputs or None if not found
        """
        try:
            logger.info(
                "Extracting node output",
                node_ids=node_ids,
            )
            nodes_by_parent_workflow = self._group_nodes_by_parent_workflow(node_ids)
            node_outputs = {}
            # Process each parent workflow
            for (
                parent_workflow_name,
                child_node_ids,
            ) in nodes_by_parent_workflow.items():
                if parent_workflow_name == MAIN_WORKFLOW_IDENTIFIER:
                    # Main workflow nodes - extract directly
                    for node_id in child_node_ids:
                        output = temporal_history.get_node_output(node_id)
                        node_outputs[node_id] = output
                    continue

                # Child workflow nodes - extract execution info from parent's history
                child_workflow_execution_info = (
                    temporal_history.get_child_workflow_execution_info(
                        parent_workflow_name
                    )
                )
                if not child_workflow_execution_info:
                    # No child workflow execution info found in parent's history
                    for node_id in child_node_ids:
                        node_outputs[node_id] = None
                    continue

                child_workflow_id, child_run_id = child_workflow_execution_info

                # Fetch child workflow history for all its children at once
                child_workflow_history = await self._fetch_temporal_history(
                    node_ids=child_node_ids,
                    workflow_id=child_workflow_id,
                    run_id=child_run_id,
                )

                if not child_workflow_history:
                    # Failed to fetch child history
                    for node_id in child_node_ids:
                        node_outputs[node_id] = None
                    continue

                child_workflow_nodes_data = child_workflow_history.get_nodes_data()

                # Extract outputs for all children
                for node_id in child_node_ids:
                    if node_id in child_workflow_nodes_data:
                        child_node_data = child_workflow_nodes_data[node_id]
                        node_outputs[node_id] = child_node_data.output_payload
                    else:
                        node_outputs[node_id] = None

            return node_outputs

        except Exception as e:
            logger.error(
                "Error extracting node outputs",
                error=str(e),
                error_type=type(e).__name__,
                node_ids=node_ids,
            )

    def _group_nodes_by_parent_workflow(
        self, node_ids: List[str]
    ) -> Dict[str, List[str]]:
        """
        Group node IDs by their immediate parent workflow.

        Groups nodes by the workflow that directly contains them:
        - Main workflow: 'activity#1' -> parent = 'main'
        - Child workflow: 'Child#1.activity#1' -> parent = 'Child#1' (Child#1's workflow_id/run_id found in main's history)
        - Nested child: 'Parent#1.Child#1.activity#1' -> parent = 'Child#1' (Child#1's workflow_id/run_id found in Parent#1's history)

        Args:
            node_ids: List of node execution IDs

        Returns:
            Dictionary mapping parent_workflow_name -> list of node_ids, ordered by workflow depth
        """
        nodes_by_parent_workflow = {}

        # Sort node_ids by depth (number of dots) to ensure parent workflows are processed first
        # we process "Child#1.activity#1" first, then "Child#1.AnotherChild#1.activity#1"
        sorted_node_ids = sorted(node_ids, key=lambda x: x.count("."))

        for node_id in sorted_node_ids:
            parts = node_id.split(".")

            if len(parts) == 1:
                # Main workflow node
                parent_workflow_name = MAIN_WORKFLOW_IDENTIFIER
            else:
                # Child workflow node - parent is the immediate parent (last part before the activity)
                parent_workflow_name = parts[-2]  # Second to last part

            if parent_workflow_name not in nodes_by_parent_workflow:
                nodes_by_parent_workflow[parent_workflow_name] = []

            nodes_by_parent_workflow[parent_workflow_name].append(node_id)

        return nodes_by_parent_workflow
