"""
Registry of supported source adapters.

The ingestion engine asks this registry for the adapter corresponding
to the source type defined in the YAML configuration.
"""

from sources import sqlserver_source


SOURCE_ADAPTERS = {
    "sqlserver": sqlserver_source,
}


def get_source_adapter(source_type: str):
    """
    Return the adapter module for a configured source type.
    """
    source_type = source_type.lower()

    if source_type not in SOURCE_ADAPTERS:
        raise NotImplementedError(
            f"Source type '{source_type}' is not implemented."
        )

    return SOURCE_ADAPTERS[source_type]