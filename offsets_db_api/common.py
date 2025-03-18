from offsets_db_api.models import Credit, Project, ProjectType


def build_filters(
    *,
    project_filters=None,
    credit_filters=None,
    project_type=None,
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
    project_type : list, optional
        Project type filters (already expanded)
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
        if 'registry' not in exclude_filters:
            filters.append(('registry', project_filters.registry, 'ilike', Project))
        if 'country' not in exclude_filters:
            filters.append(('country', project_filters.country, 'ilike', Project))
        if 'protocol' not in exclude_filters:
            filters.append(('protocol', project_filters.protocol, 'ANY', Project))
        if 'category' not in exclude_filters:
            filters.append(('category', project_filters.category, 'ANY', Project))
        if 'is_compliance' not in exclude_filters:
            filters.append(('is_compliance', project_filters.is_compliance, '==', Project))
        if 'listed_at' not in exclude_filters:
            filters.append(('listed_at', project_filters.listed_at_from, '>=', Project))
            filters.append(('listed_at', project_filters.listed_at_to, '<=', Project))
        if 'issued' not in exclude_filters:
            filters.append(('issued', project_filters.issued_min, '>=', Project))
            filters.append(('issued', project_filters.issued_max, '<=', Project))
        if 'retired' not in exclude_filters:
            filters.append(('retired', project_filters.retired_min, '>=', Project))
            filters.append(('retired', project_filters.retired_max, '<=', Project))

    # Credit filters
    if credit_filters:
        if 'transaction_type' not in exclude_filters:
            filters.append(('transaction_type', credit_filters.transaction_type, 'ilike', Credit))
        if 'vintage' not in exclude_filters:
            filters.append(('vintage', credit_filters.vintage, '==', Credit))
        if 'transaction_date' not in exclude_filters:
            filters.append(('transaction_date', credit_filters.transaction_date_from, '>=', Credit))
            filters.append(('transaction_date', credit_filters.transaction_date_to, '<=', Credit))

    # Project type filters
    if project_type and 'project_type' not in exclude_filters:
        filters.append(('project_type', project_type, 'ilike', ProjectType))

    return filters
