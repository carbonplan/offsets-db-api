from dataclasses import dataclass
from datetime import datetime

from offsets_db_api.common import build_filters
from offsets_db_api.models import Clip, ClipProject, Credit, Project


@dataclass
class MockProjectFilters:
    registry: str | None = None
    country: str | None = None
    protocol: list[str] | None = None
    category: str | None = None
    project_type: str | None = None
    is_compliance: bool | None = None
    listed_at_from: datetime | None = None
    listed_at_to: datetime | None = None
    issued_min: int | None = None
    issued_max: int | None = None
    retired_min: int | None = None
    retired_max: int | None = None


@dataclass
class MockCreditFilters:
    transaction_type: str | None = None
    vintage: int | None = None
    transaction_date_from: datetime | None = None
    transaction_date_to: datetime | None = None


@dataclass
class MockClipFilters:
    type: str | None = None
    source: str | None = None
    tags: list[str] | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    project_id: str | None = None


def test_build_filters_empty():
    """Test that empty filters return an empty list."""
    filters = build_filters()
    assert filters == []


def test_build_filters_project():
    """Test building filters with project parameters."""
    project_filters = MockProjectFilters(
        registry='verra',
        country='US',
        protocol=['AFOLU', 'REDD+'],
        category='forestry',
        project_type='conservation',
        is_compliance=False,
        listed_at_from=datetime(2020, 1, 1),
        listed_at_to=datetime(2022, 1, 1),
        issued_min=1000,
        issued_max=10000,
        retired_min=100,
        retired_max=1000,
    )

    filters = build_filters(project_filters=project_filters)

    # Verify all project filters are present
    assert len(filters) == 12  # 6 single filters + 3 pairs (from/to ranges)

    # Check specific filters
    assert ('registry', 'verra', 'ilike', Project) in filters
    assert ('country', 'US', 'ilike', Project) in filters
    assert ('protocol', ['AFOLU', 'REDD+'], 'ANY', Project) in filters
    assert ('category', 'forestry', 'ilike', Project) in filters
    assert ('project_type', 'conservation', 'ilike', Project) in filters
    assert ('is_compliance', False, '==', Project) in filters

    # Check range filters
    assert ('listed_at', datetime(2020, 1, 1), '>=', Project) in filters
    assert ('listed_at', datetime(2022, 1, 1), '<=', Project) in filters
    assert ('issued', 1000, '>=', Project) in filters
    assert ('issued', 10000, '<=', Project) in filters
    assert ('retired', 100, '>=', Project) in filters
    assert ('retired', 1000, '<=', Project) in filters


def test_build_filters_credit():
    """Test building filters with credit parameters."""
    credit_filters = MockCreditFilters(
        transaction_type='issuance',
        vintage=2020,
        transaction_date_from=datetime(2020, 1, 1),
        transaction_date_to=datetime(2021, 1, 1),
    )

    filters = build_filters(credit_filters=credit_filters)

    # Verify all credit filters are present
    assert len(filters) == 4  # 2 single filters + 1 pair (date range)

    # Check specific filters
    assert ('transaction_type', 'issuance', 'ilike', Credit) in filters
    assert ('vintage', 2020, '==', Credit) in filters
    assert ('transaction_date', datetime(2020, 1, 1), '>=', Credit) in filters
    assert ('transaction_date', datetime(2021, 1, 1), '<=', Credit) in filters


def test_build_filters_clip():
    """Test building filters with clip parameters."""
    clip_filters = MockClipFilters(
        type='news',
        source='website',
        tags=['climate', 'carbon'],
        date_from=datetime(2020, 1, 1),
        date_to=datetime(2021, 1, 1),
        project_id='PROJ123',
    )

    filters = build_filters(clip_filters=clip_filters)

    # Verify all clip filters are present
    assert len(filters) == 6  # 3 single filters + 1 pair (date range) + 1 project_id

    # Check specific filters
    assert ('type', 'news', 'ilike', Clip) in filters
    assert ('source', 'website', 'ilike', Clip) in filters
    assert ('tags', ['climate', 'carbon'], 'ANY', Clip) in filters
    assert ('date', datetime(2020, 1, 1), '>=', Clip) in filters
    assert ('date', datetime(2021, 1, 1), '<=', Clip) in filters
    assert ('project_id', 'PROJ123', '==', ClipProject) in filters


def test_build_filters_exclude():
    """Test excluding specific filters."""
    project_filters = MockProjectFilters(
        registry='verra', country='US', protocol=['AFOLU'], is_compliance=False
    )

    # Exclude registry and country
    filters = build_filters(
        project_filters=project_filters, exclude_filters=['registry', 'country']
    )

    # Should only have protocol and is_compliance
    assert len(filters) == 2
    assert ('registry', 'verra', 'ilike', Project) not in filters
    assert ('country', 'US', 'ilike', Project) not in filters
    assert ('protocol', ['AFOLU'], 'ANY', Project) in filters
    assert ('is_compliance', False, '==', Project) in filters


def test_build_filters_combined():
    """Test building filters with multiple filter types."""
    project_filters = MockProjectFilters(registry='verra', is_compliance=False)
    credit_filters = MockCreditFilters(transaction_type='issuance', vintage=2020)
    clip_filters = MockClipFilters(source='website', project_id='PROJ123')

    filters = build_filters(
        project_filters=project_filters, credit_filters=credit_filters, clip_filters=clip_filters
    )

    assert len(filters) == 6

    assert ('registry', 'verra', 'ilike', Project) in filters
    assert ('is_compliance', False, '==', Project) in filters

    assert ('transaction_type', 'issuance', 'ilike', Credit) in filters
    assert ('vintage', 2020, '==', Credit) in filters

    assert ('source', 'website', 'ilike', Clip) in filters
    assert ('project_id', 'PROJ123', '==', ClipProject) in filters


def test_build_filters_exclude_complex():
    """Test excluding filters in a complex scenario."""
    project_filters = MockProjectFilters(
        registry='verra', issued_min=1000, issued_max=10000, retired_min=100, retired_max=1000
    )
    credit_filters = MockCreditFilters(transaction_type='issuance', vintage=2020)

    filters = build_filters(
        project_filters=project_filters,
        credit_filters=credit_filters,
        exclude_filters=['issued', 'vintage'],
    )

    # Should have registry, retired_min/max, and transaction_type
    assert len(filters) == 4
    assert ('registry', 'verra', 'ilike', Project) in filters
    assert ('issued', 1000, '>=', Project) not in filters
    assert ('issued', 10000, '<=', Project) not in filters
    assert ('retired', 100, '>=', Project) in filters
    assert ('retired', 1000, '<=', Project) in filters
    assert ('transaction_type', 'issuance', 'ilike', Credit) in filters
    assert ('vintage', 2020, '==', Credit) not in filters
