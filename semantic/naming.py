def strip_source_suffix(table_name: str) -> str:
    return (
        table_name.upper()
        .replace("_QAD", "")
        .replace("_E4", "")
        .replace("_ENDEAV", "")
    )


def strip_source_prefix(table_name: str) -> str:
    return strip_source_suffix(table_name).replace("IDG_", "")


def source_short_name(source_system: str) -> str:
    return (
        source_system.upper()
        .replace("_REDSHIFT", "")
        .replace("_SQLSERVER", "")
    )


def infer_entity_name(table_name: str) -> str:
    table = table_name.upper()

    if "ESTIMATE_LINE" in table or "SARASOTA_ESTIMATE_LINE" in table:
        return "ESTIMATE_LINE"
    if "INVOICE" in table:
        return "INVOICE"
    if "ORDER" in table:
        return "ORDER"
    if "ESTIMATE" in table:
        return "ESTIMATE"
    if "CONTACT" in table:
        return "CONTACT"
    if "CUSTOMER" in table:
        return "CUSTOMER"
    if "CAMPUS" in table:
        return "CAMPUS"

    return strip_source_prefix(table)


def make_entity_view_name(entity_name: str, source_system: str) -> str:
    return f"V_{entity_name}_{source_short_name(source_system)}"


def make_relationship_view_name(
    from_table: str,
    to_table: str,
    source_system: str,
) -> str:
    return (
        f"V_{strip_source_prefix(from_table)}"
        f"_WITH_{strip_source_prefix(to_table)}"
        f"_{source_short_name(source_system)}"
    )


def make_column_alias(table_name: str, column_name: str) -> str:
    prefix = strip_source_prefix(table_name)
    return f"{prefix}_{column_name.upper()}"