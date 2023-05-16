from fastapi import HTTPException
from sqlalchemy import asc, desc


def apply_sorting(*, query, sort: list[str], model):
    ascending_order = ['ascending', 'descending', 'asc', 'desc']
    columns = [c.name for c in model.__table__.columns]

    for sort_param in sort:
        # if sort_param not in the form of 'property:order' raise an error
        if ':' not in sort_param:
            raise HTTPException(
                status_code=400,
                detail=f'Invalid sort parameter: {sort_param}. Must be of the form "property:order"',
            )
        property, order = sort_param.split(':')

        if property not in columns:
            raise HTTPException(
                status_code=400,
                detail=f'Invalid sort property: {property}. Must be one of {columns}',
            )

        if order in ascending_order:
            query = (
                query.order_by(asc(getattr(model, property)))
                if order in ['ascending', 'asc']
                else query.order_by(desc(getattr(model, property)))
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=f'Invalid sort order: {order}. Must be one of {ascending_order}',
            )

    return query
