from offsets_db_api.models import Clip, ClipProject, Credit, Project


def build_filters(
    *,
    project_filters=None,
    credit_filters=None,
    clip_filters=None,
    exclude_filters=None,
):
    """
    Build a standardized list of filters for SQL queries.

    Parameters
    ----------
    project_filters : ProjectFilters, optional
        Project filter parameters
    credit_filters : CreditFilters, optional
        Credit filter parameters
    clip_filters : ClipFilters, optional
        Clip filter parameters
    exclude_filters : list, optional
        List of filter names to exclude (e.g. ['issued', 'retired'])

    Returns
    -------
    list
        List of (attribute, values, operation, model) tuples for filtering
    """

    exclude_filters = exclude_filters or []
    filters = []

    # Project filters
    if project_filters:
        if 'registry' not in exclude_filters and project_filters.registry is not None:
            filters.append(('registry', project_filters.registry, 'ilike', Project))
        if 'country' not in exclude_filters and project_filters.country is not None:
            filters.append(('country', project_filters.country, 'ilike', Project))
        if 'protocol' not in exclude_filters and project_filters.protocol is not None:
            filters.append(('protocol', project_filters.protocol, 'ANY', Project))
        if 'category' not in exclude_filters and project_filters.category is not None:
            filters.append(('category', project_filters.category, 'ilike', Project))
        if 'type' not in exclude_filters and project_filters.type is not None:
            filters.append(('type', project_filters.type, 'ilike', Project))
        if 'is_compliance' not in exclude_filters and project_filters.is_compliance is not None:
            filters.append(('is_compliance', project_filters.is_compliance, '==', Project))
        if 'listed_at' not in exclude_filters:
            if project_filters.listed_at_from is not None:
                filters.append(('listed_at', project_filters.listed_at_from, '>=', Project))
            if project_filters.listed_at_to is not None:
                filters.append(('listed_at', project_filters.listed_at_to, '<=', Project))
        if 'issued' not in exclude_filters:
            if project_filters.issued_min is not None:
                filters.append(('issued', project_filters.issued_min, '>=', Project))
            if project_filters.issued_max is not None:
                filters.append(('issued', project_filters.issued_max, '<=', Project))
        if 'retired' not in exclude_filters:
            if project_filters.retired_min is not None:
                filters.append(('retired', project_filters.retired_min, '>=', Project))
            if project_filters.retired_max is not None:
                filters.append(('retired', project_filters.retired_max, '<=', Project))

    # Credit filters
    if credit_filters:
        if (
            'transaction_type' not in exclude_filters
            and credit_filters.transaction_type is not None
        ):
            filters.append(('transaction_type', credit_filters.transaction_type, 'ilike', Credit))
        if 'vintage' not in exclude_filters and credit_filters.vintage is not None:
            filters.append(('vintage', credit_filters.vintage, '==', Credit))
        if 'transaction_date' not in exclude_filters:
            if credit_filters.transaction_date_from is not None:
                filters.append(
                    ('transaction_date', credit_filters.transaction_date_from, '>=', Credit)
                )
            if credit_filters.transaction_date_to is not None:
                filters.append(
                    ('transaction_date', credit_filters.transaction_date_to, '<=', Credit)
                )

    # Clip filters
    if clip_filters:
        if 'type' not in exclude_filters and clip_filters.type is not None:
            filters.append(('type', clip_filters.type, 'ilike', Clip))
        if 'source' not in exclude_filters and clip_filters.source is not None:
            filters.append(('source', clip_filters.source, 'ilike', Clip))
        if 'tags' not in exclude_filters and clip_filters.tags is not None:
            filters.append(('tags', clip_filters.tags, 'ANY', Clip))
        if 'date' not in exclude_filters:
            if clip_filters.date_from is not None:
                filters.append(('date', clip_filters.date_from, '>=', Clip))
            if clip_filters.date_to is not None:
                filters.append(('date', clip_filters.date_to, '<=', Clip))
        if 'project_id' not in exclude_filters and clip_filters.project_id is not None:
            filters.append(('project_id', clip_filters.project_id, '==', ClipProject))

    return filters
