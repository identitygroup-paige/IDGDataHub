from semantic.naming import make_column_alias


def quote_identifier(name: str) -> str:
    return f'"{name}"'


def build_entity_select_columns(columns_df) -> str:
    lines = []

    for _, row in columns_df.iterrows():
        column = row["TARGET_COLUMN"]
        lines.append(f"    {quote_identifier(column)}")

    return ",\n".join(lines)


def build_parent_select_columns(parent_columns_df) -> str:
    lines = []

    for _, row in parent_columns_df.iterrows():
        column = row["TARGET_COLUMN"]
        lines.append(f"    p.{quote_identifier(column)}")

    return ",\n".join(lines)


def build_child_select_columns(child_columns_df, parent_column_names, child_table) -> str:
    lines = []

    for _, row in child_columns_df.iterrows():
        column = row["TARGET_COLUMN"]

        if column in parent_column_names:
            alias = make_column_alias(child_table, column)
            lines.append(f"    c.{quote_identifier(column)} AS {quote_identifier(alias)}")
        else:
            lines.append(f"    c.{quote_identifier(column)}")

    return ",\n".join(lines)