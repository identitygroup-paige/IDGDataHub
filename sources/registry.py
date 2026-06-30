"""
Registry of supported source adapters.

The ingestion engine asks this registry for the adapter corresponding
to the source type defined in the YAML configuration.
"""

from sources import redshift_source, sqlserver_source


SOURCE_ADAPTERS = {
    "sqlserver": sqlserver_source,
    "redshift": redshift_source,
}


def get_source_adapter(source_type: str):
    try:
        return SOURCE_ADAPTERS[source_type.lower()]
    except KeyError as error:
        supported = ", ".join(sorted(SOURCE_ADAPTERS))
        raise ValueError(
            f"Unsupported source type: {source_type}. "
            f"Supported source types: {supported}"
        ) from error