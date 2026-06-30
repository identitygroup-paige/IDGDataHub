from semantic.column_selector import (
    build_child_select_columns,
    build_parent_select_columns,
)


def build_flat_relationship_select(
    parent_columns_df,
    child_columns_df,
    child_table: str,
) -> dict:
    parent_column_names = set(parent_columns_df["TARGET_COLUMN"].tolist())

    return {
        "parent_select_columns": build_parent_select_columns(parent_columns_df),
        "child_select_columns": build_child_select_columns(
            child_columns_df=child_columns_df,
            parent_column_names=parent_column_names,
            child_table=child_table,
        ),
    }