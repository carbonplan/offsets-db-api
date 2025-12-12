"""
Helper functions for loading geographic data from parquet files.
"""

import functools

import pandas as pd

from offsets_db_api.log import get_logger

logger = get_logger()

# S3 URL for project boundaries geoparquet
PROJECT_BOUNDARIES_URL = 's3://carbonplan-offsets-db/miscellaneous/project-boundaries.parquet'


@functools.lru_cache(maxsize=1)
def load_project_bboxes() -> dict[str, dict[str, float]]:
    """
    Load project bounding boxes from the geoparquet file.

    Returns a dictionary mapping project_id to bbox dict with keys:
    xmin, ymin, xmax, ymax

    The result is cached to avoid repeated S3 reads.
    """
    try:
        logger.info(f'Loading project bboxes from {PROJECT_BOUNDARIES_URL}')
        df = pd.read_parquet(
            PROJECT_BOUNDARIES_URL,
            columns=['project_id', 'bbox'],
            storage_options={'anon': True},
        )

        # Convert to dict mapping project_id -> bbox
        bbox_lookup = {}
        for _, row in df.iterrows():
            project_id = row['project_id']
            bbox = row['bbox']
            if bbox is not None:
                bbox_lookup[project_id] = {
                    'xmin': bbox.get('xmin'),
                    'ymin': bbox.get('ymin'),
                    'xmax': bbox.get('xmax'),
                    'ymax': bbox.get('ymax'),
                }

        logger.info(f'Loaded {len(bbox_lookup)} project bboxes')
        return bbox_lookup

    except Exception as e:
        logger.error(f'Failed to load project bboxes: {e}')
        return {}


def get_bbox_for_project(project_id: str) -> dict[str, float] | None:
    """
    Get the bounding box for a specific project.

    Parameters
    ----------
    project_id : str
        The project ID to look up

    Returns
    -------
    dict or None
        Bbox dict with xmin, ymin, xmax, ymax keys, or None if not found
    """
    bbox_lookup = load_project_bboxes()
    return bbox_lookup.get(project_id)


def get_bboxes_for_projects(project_ids: list[str]) -> dict[str, dict[str, float]]:
    """
    Get bounding boxes for multiple projects.

    Parameters
    ----------
    project_ids : list of str
        List of project IDs to look up

    Returns
    -------
    dict
        Dictionary mapping project_id to bbox dict
    """
    bbox_lookup = load_project_bboxes()
    return {pid: bbox_lookup[pid] for pid in project_ids if pid in bbox_lookup}


def get_projects_with_geometry() -> set[str]:
    """
    Get the set of project IDs that have geographic boundaries.

    Returns
    -------
    set
        Set of project IDs that have boundaries
    """
    bbox_lookup = load_project_bboxes()
    return set(bbox_lookup.keys())


def clear_bbox_cache():
    """Clear the cached bbox data to force a reload."""
    load_project_bboxes.cache_clear()
    logger.info('Cleared project bbox cache')
