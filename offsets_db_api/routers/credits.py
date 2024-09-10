import datetime
import json
import re

from fastapi import APIRouter, Depends, Query, Request
from fastapi_cache.decorator import cache
from pydantic import BaseModel
from sqlmodel import Session, col, func, or_, select

from offsets_db_api.cache import CACHE_NAMESPACE
from offsets_db_api.database import get_session
from offsets_db_api.log import get_logger
from offsets_db_api.models import Credit, PaginatedCredits, Project
from offsets_db_api.schemas import Pagination, Registries
from offsets_db_api.security import check_api_key
from offsets_db_api.sql_helpers import apply_filters, apply_sorting, handle_pagination

router = APIRouter()
logger = get_logger()

# Helper functions


def normalize_text(text: str) -> str:
    return re.sub(r'[^\w\s]', '', text.lower()).strip()


ACRONYM_EXPANSIONS = {
    'jp': ['j p', 'j.p.'],
    'ms': ['microsoft'],
    'ibm': ['i b m', 'i.b.m.'],
    # Add more acronym expansions as needed
}


def expand_acronyms(text: str) -> list[str]:
    words = text.split()
    expansions = [text]

    for i, word in enumerate(words):
        if word in ACRONYM_EXPANSIONS:
            for expansion in ACRONYM_EXPANSIONS[word]:
                new_words = words.copy()
                new_words[i] = expansion
                expansions.append(' '.join(new_words))

    return expansions


COMPANY_ALIASES = {
    'apple': ['apple inc', 'apple incorporated'],
    'jp morgan': ['jpmorgan', 'jp morgan chase', 'chase bank', 'j p morgan', 'j.p. morgan'],
    'microsoft': ['microsoft corporation', 'ms'],
    # Add more aliases as needed
}


def get_aliases(term: str) -> list[str]:
    normalized_term = normalize_text(term)
    return next(
        (
            [key] + aliases
            for key, aliases in COMPANY_ALIASES.items()
            if normalized_term in [normalize_text(a) for a in [key] + aliases]
        ),
        [normalized_term],
    )


class SearchField(BaseModel):
    field: str
    weight: float


def parse_search_fields(
    search_fields_str: str = '[{"field":"retirement_beneficiary","weight":2.0},{"field":"retirement_account","weight":1.5},{"field":"retirement_note","weight":1.0},{"field":"retirement_reason","weight":1.0}]',
) -> list[SearchField]:
    try:
        search_fields = json.loads(search_fields_str)
        return [SearchField(**field) for field in search_fields]
    except json.JSONDecodeError:
        raise ValueError('Invalid JSON format for search_fields')
    except KeyError:
        raise ValueError("Each search field must have 'field' and 'weight' keys")


# Main endpoint


@router.get('/', summary='List credits', response_model=PaginatedCredits)
@cache(namespace=CACHE_NAMESPACE)
async def get_credits(
    request: Request,
    project_id: list[str] | None = Query(None, description='Project ID'),
    registry: list[Registries] | None = Query(None, description='Registry name'),
    category: list[str] | None = Query(None, description='Category name'),
    is_compliance: bool | None = Query(None, description='Whether project is an ARB project'),
    transaction_type: list[str] | None = Query(None, description='Transaction type'),
    vintage: list[int] | None = Query(None, description='Vintage'),
    transaction_date_from: datetime.datetime | datetime.date | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    transaction_date_to: datetime.datetime | datetime.date | None = Query(
        default=None, description='Format: YYYY-MM-DD'
    ),
    search: str | None = Query(
        None,
        description='Search string. Use "r:" prefix for regex search, "t:" for trigram search, "w:" for weighted search, or leave blank for case-insensitive partial match.',
    ),
    search_fields: str = Query(
        default='[{"field":"retirement_beneficiary","weight":2.0},{"field":"retirement_account","weight":1.5},{"field":"retirement_note","weight":1.0},{"field":"retirement_reason","weight":1.0}]',
        description='JSON string of fields to search in with their weights',
    ),
    similarity_threshold: float = Query(
        0.7, ge=0.0, le=1.0, description='similarity threshold (0-1)'
    ),
    sort: list[str] = Query(
        default=['project_id'],
        description='List of sorting parameters in the format `field_name` or `+field_name` for ascending order or `-field_name` for descending order.',
    ),
    current_page: int = Query(1, description='Page number', ge=1),
    per_page: int = Query(100, description='Items per page', le=200, ge=1),
    session: Session = Depends(get_session),
    authorized_user: bool = Depends(check_api_key),
):
    """List credits"""
    logger.info(f'Getting credits: {request.url}')

    # Outer join to get all credits, even if they don't have a project
    statement = select(Credit, Project.category).join(
        Project, col(Credit.project_id) == col(Project.project_id), isouter=True
    )

    filters = [
        ('registry', registry, 'ilike', Project),
        ('transaction_type', transaction_type, 'ilike', Credit),
        ('category', category, 'ANY', Project),
        ('is_compliance', is_compliance, '==', Project),
        ('vintage', vintage, '==', Credit),
        ('transaction_date', transaction_date_from, '>=', Credit),
        ('transaction_date', transaction_date_to, '<=', Credit),
    ]

    # Filter for project_id
    if project_id:
        filters.insert(0, ('project_id', project_id, '==', Project))

    for attribute, values, operation, model in filters:
        statement = apply_filters(
            statement=statement,
            model=model,
            attribute=attribute,
            values=values,
            operation=operation,
        )

    # Handle advanced search
    if search:
        search_conditions = []
        logger.info(f'Search string: {search}')
        logger.info(f'Search fields: {search_fields}')

        search_fields = parse_search_fields(search_fields)

        if search.startswith('r:'):
            # Regex search
            pattern = search[2:]  # Remove 'r:' prefix
            logger.info(f'Regex search pattern: {pattern}')
            for field_info in search_fields:
                field = field_info.field
                if field in Credit.__table__.columns:
                    search_conditions.append(col(getattr(Credit, field)).op('~*')(pattern))
                elif field in Project.__table__.columns:
                    search_conditions.append(col(getattr(Project, field)).op('~*')(pattern))
        elif search.startswith('t:'):
            # Trigram similarity search
            search_term = search[2:]  # Remove 't:' prefix
            logger.info(f'Trigram search term: {search_term}')
            for field_info in search_fields:
                field = field_info.field
                if field in Credit.__table__.columns:
                    search_conditions.append(
                        func.word_similarity(func.lower(getattr(Credit, field)), search_term)
                        > similarity_threshold
                    )
                elif field in Project.__table__.columns:
                    search_conditions.append(
                        func.word_similarity(func.lower(getattr(Project, field)), search_term)
                        > similarity_threshold
                    )
        elif search.startswith('w:'):
            # Weighted search with alias and acronym expansion
            search_term = search[2:]  # Remove 'w:' prefix
            logger.info(f'Weighted search term: {search_term}')
            variations = expand_acronyms(search_term)
            variations.extend(get_aliases(search_term))

            for variation in variations:
                for field_info in search_fields:
                    field = field_info.field
                    weight = field_info.weight
                    if field in Credit.__table__.columns:
                        search_conditions.append(
                            func.similarity(func.lower(getattr(Credit, field)), variation) * weight
                            > similarity_threshold
                        )
                    elif field in Project.__table__.columns:
                        search_conditions.append(
                            func.similarity(func.lower(getattr(Project, field)), variation) * weight
                            > similarity_threshold
                        )
        else:
            # Case-insensitive partial match (default behavior)
            search_pattern = f'%{search}%'
            for field_info in search_fields:
                field = field_info.field
                if field in Credit.__table__.columns:
                    search_conditions.append(col(getattr(Credit, field)).ilike(search_pattern))
                elif field in Project.__table__.columns:
                    search_conditions.append(col(getattr(Project, field)).ilike(search_pattern))

        if search_conditions:
            statement = statement.where(or_(*search_conditions))

    if sort:
        statement = apply_sorting(statement=statement, sort=sort, model=Credit, primary_key='id')

    logger.info(f"SQL Credits Query: {statement.compile(compile_kwargs={'literal_binds': True})}")

    total_entries, current_page, total_pages, next_page, results = handle_pagination(
        statement=statement,
        primary_key=Credit.id,
        current_page=current_page,
        per_page=per_page,
        request=request,
        session=session,
    )

    credits_with_category = [
        {
            **credit.model_dump(),
            'projects': [{'project_id': credit.project_id, 'category': category}],
        }
        for credit, category in results
    ]

    return PaginatedCredits(
        pagination=Pagination(
            total_entries=total_entries,
            current_page=current_page,
            total_pages=total_pages,
            next_page=next_page,
        ),
        data=credits_with_category,
    )
