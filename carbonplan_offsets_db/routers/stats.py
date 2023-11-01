import datetime

import pandas as pd
from fastapi import APIRouter, Depends, Query, Request
from sqlmodel import Session, or_

from ..database import get_engine, get_session
from ..logging import get_logger
from ..models import PaginatedCreditCounts, PaginatedProjectCounts, Project
from ..query_helpers import apply_filters
from ..schemas import Pagination, Registries
from ..settings import get_settings

router = APIRouter()
logger = get_logger()


def filter_valid_projects(df: pd.DataFrame, categories: list | None = None) -> pd.DataFrame:
    if not categories:
        # If no categories are provided, return all of them
        return df
    # Filter the dataframe to include only rows with the specified categories
    valid_projects = df[df['category'].isin(categories)]

    # Group by project and filter out projects that have different categories outside the given list
    grouped = valid_projects.groupby('project_id')
    valid_project_ids = grouped.filter(
        lambda x: x['category'].nunique() == len(x)
    ).project_id.unique()
    return valid_projects[valid_projects['project_id'].isin(valid_project_ids)]


def projects_by_category(
    *, df: pd.DataFrame, categories: list | None = None
) -> list[dict[str, int]]:
    valid_projects = filter_valid_projects(df, categories)
    counts = valid_projects.groupby('category').count()['project_id']
    return [{'category': category, 'value': count} for category, count in counts.items()]


def credits_by_category(
    *, df: pd.DataFrame, categories: list | None = None
) -> list[dict[str, int]]:
    valid_projects = filter_valid_projects(df, categories)
    credits = (
        valid_projects.groupby('category').agg({'issued': 'sum', 'retired': 'sum'}).reset_index()
    )
    return [
        {'category': row['category'], 'issued': row['issued'], 'retired': row['retired']}
        for _, row in credits.iterrows()
    ]


@router.get('/projects_by_category', response_model=PaginatedProjectCounts)
def get_projects_by_category(
    request: Request,
    registry: list[Registries] | None = Query(None, description='Registry name'),
    country: list[str] | None = Query(None, description='Country name'),
    protocol: list[str] | None = Query(None, description='Protocol name'),
    category: list[str] | None = Query(None, description='Category name'),
    is_compliance: bool | None = Query(None, description='Whether project is an ARB project'),
    listed_at_from: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    listed_at_to: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    issued_min: int | None = Query(None, description='Minimum number of issued credits'),
    issued_max: int | None = Query(None, description='Maximum number of issued credits'),
    retired_min: int | None = Query(None, description='Minimum number of retired credits'),
    retired_max: int | None = Query(None, description='Maximum number of retired credits'),
    search: str
    | None = Query(
        None,
        description='Case insensitive search string. Currently searches on `project_id` and `name` fields only.',
    ),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    session: Session = Depends(get_session),
):
    """Get project counts by category"""
    logger.info(f'Getting project count by category: {request.url}')

    query = session.query(Project)

    filters = [
        ('registry', registry, 'ilike', Project),
        ('country', country, 'ilike', Project),
        ('protocol', protocol, 'ANY', Project),
        ('category', category, 'ANY', Project),
        ('is_compliance', is_compliance, '==', Project),
        ('listed_at', listed_at_from, '>=', Project),
        ('listed_at', listed_at_to, '<=', Project),
        ('issued', issued_min, '>=', Project),
        ('issued', issued_max, '<=', Project),
        ('retired', retired_min, '>=', Project),
        ('retired', retired_max, '<=', Project),
    ]

    for attribute, values, operation, model in filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        query = query.filter(
            or_(Project.project_id.ilike(search_pattern), Project.name.ilike(search_pattern))
        )

    settings = get_settings()
    engine = get_engine(database_url=settings.database_url)

    df = pd.read_sql_query(query.statement, engine).explode('category')
    logger.info(f'Sample of the dataframe with size: {df.shape}\n{df.head()}')
    results = projects_by_category(df=df, categories=category)

    return PaginatedProjectCounts(
        data=results,
        pagination=Pagination(
            current_page=current_page, per_page=per_page, total_entries=len(results), total_pages=1
        ),
    )


@router.get('/credits_by_category', response_model=PaginatedCreditCounts)
def get_credits_by_category(
    request: Request,
    registry: list[Registries] | None = Query(None, description='Registry name'),
    country: list[str] | None = Query(None, description='Country name'),
    protocol: list[str] | None = Query(None, description='Protocol name'),
    category: list[str] | None = Query(None, description='Category name'),
    is_compliance: bool | None = Query(None, description='Whether project is an ARB project'),
    listed_at_from: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    listed_at_to: datetime.date
    | datetime.datetime
    | None = Query(default=None, description='Format: YYYY-MM-DD'),
    issued_min: int | None = Query(None, description='Minimum number of issued credits'),
    issued_max: int | None = Query(None, description='Maximum number of issued credits'),
    retired_min: int | None = Query(None, description='Minimum number of retired credits'),
    retired_max: int | None = Query(None, description='Maximum number of retired credits'),
    search: str
    | None = Query(
        None,
        description='Case insensitive search string. Currently searches on `project_id` and `name` fields only.',
    ),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    session: Session = Depends(get_session),
):
    """Get project counts by category"""
    logger.info(f'Getting project count by category: {request.url}')

    query = session.query(Project)

    filters = [
        ('registry', registry, 'ilike', Project),
        ('country', country, 'ilike', Project),
        ('protocol', protocol, 'ANY', Project),
        ('category', category, 'ANY', Project),
        ('is_compliance', is_compliance, '==', Project),
        ('listed_at', listed_at_from, '>=', Project),
        ('listed_at', listed_at_to, '<=', Project),
        ('issued', issued_min, '>=', Project),
        ('issued', issued_max, '<=', Project),
        ('retired', retired_min, '>=', Project),
        ('retired', retired_max, '<=', Project),
    ]

    for attribute, values, operation, model in filters:
        query = apply_filters(
            query=query, model=model, attribute=attribute, values=values, operation=operation
        )

    # Handle 'search' filter separately due to its unique logic
    if search:
        search_pattern = f'%{search}%'
        query = query.filter(
            or_(Project.project_id.ilike(search_pattern), Project.name.ilike(search_pattern))
        )

    settings = get_settings()
    engine = get_engine(database_url=settings.database_url)

    df = pd.read_sql_query(query.statement, engine).explode('category')
    logger.info(f'Sample of the dataframe with size: {df.shape}\n{df.head()}')
    results = credits_by_category(df=df, categories=category)

    return PaginatedCreditCounts(
        data=results,
        pagination=Pagination(
            current_page=current_page, per_page=per_page, total_entries=len(results), total_pages=1
        ),
    )
