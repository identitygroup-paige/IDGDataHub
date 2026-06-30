def infer_entity_name(table_name: str) -> str:
    table = table_name.upper()

    if "ESTIMATE_LINE" in table or "ESTIMATE_LINE" in table.replace("SARASOTA_", ""):
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

    return table.replace("_QAD", "").replace("_E4", "").replace("_ENDEAV", "")


def make_view_name(entity_name: str, source_system: str) -> str:
    clean_source = source_system.upper().replace("_REDSHIFT", "").replace("_SQLSERVER", "")
    return f"V_{entity_name}_{clean_source}"