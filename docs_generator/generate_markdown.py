import argparse
from pathlib import Path

from semantic.catalog import SemanticCatalog


def format_list(values):
    if not values:
        return "- None discovered"
    return "\n".join(f"- {value}" for value in values)


def write_readme(catalog: SemanticCatalog, output_dir: Path):
    counts = catalog.counts()

    content = f"""# {catalog.source_system} Semantic Model

Generated: `{catalog.generated_at}`

## Summary

| Asset Type | Count |
|---|---:|
| Entity Views | {counts["entity_views"]} |
| Safe Relationship Views | {counts["safe_relationship_views"]} |
| Bridge Views | {counts["bridge_views"]} |
| Summary Views | {counts["summary_views"]} |
| Skipped Relationships | {counts["skipped_relationships"]} |

## Entities

| Entity | Source Table | View |
|---|---|---|
"""

    for entity in catalog.entity_views():
        content += (
            f"| {entity['entity_name']} | "
            f"{entity['source_table']} | "
            f"{entity['view_name']} |\n"
        )

    content += "\n## Next Files\n\n"
    content += "- [Relationships](relationships.md)\n"
    content += "- Entity pages are in the `entities/` folder.\n"

    (output_dir / "README.md").write_text(content)


def write_entity_pages(catalog: SemanticCatalog, output_dir: Path):
    entity_dir = output_dir / "entities"
    entity_dir.mkdir(parents=True, exist_ok=True)

    for entity in catalog.entity_views():
        file_name = f"{entity['entity_name'].lower()}.md"

        content = f"""# {entity['entity_name']}

## View

`{entity['view_name']}`

## Source Table

`{entity['source_table']}`

## Primary Keys

{format_list(entity.get("primary_keys", []))}

## Business Keys

{format_list(entity.get("business_keys", []))}

## Unique Attributes

{format_list(entity.get("unique_attributes", []))}
"""

        (entity_dir / file_name).write_text(content)


def write_relationships(catalog: SemanticCatalog, output_dir: Path):
    content = "# Relationships\n\n"

    content += "## Safe Flat Relationship Views\n\n"
    if catalog.safe_relationship_views():
        for rel in catalog.safe_relationship_views():
            content += relationship_block(rel)
    else:
        content += "None discovered.\n\n"

    content += "## Bridge Views\n\n"
    if catalog.bridge_views():
        for rel in catalog.bridge_views():
            content += relationship_block(rel)
    else:
        content += "None discovered.\n\n"

    content += "## Summary Views\n\n"
    if catalog.summary_views():
        for rel in catalog.summary_views():
            content += relationship_block(rel)
    else:
        content += "None discovered.\n\n"

    content += "## Skipped Unsafe Relationships\n\n"
    if catalog.skipped_relationships():
        for rel in catalog.skipped_relationships():
            content += relationship_block(rel, include_skip=True)
    else:
        content += "None.\n\n"

    (output_dir / "relationships.md").write_text(content)


def relationship_block(rel, include_skip=False):
    content = f"""### {rel['from_table']} → {rel['to_table']}

| Field | Value |
|---|---|
| From Column | `{rel['from_column']}` |
| To Column | `{rel['to_column']}` |
| Confidence | {rel['confidence_label']} |
| Match Rate | {rel['match_rate']:.4f} |
| Cardinality | {rel['cardinality_type']} |
"""

    if "view_name" in rel:
        content += f"| View | `{rel['view_name']}` |\n"

    if include_skip:
        content += f"| Skip Reason | {rel.get('skip_reason', '')} |\n"
        content += f"| Unsafe Flat View | `{rel.get('unsafe_flat_view_name', '')}` |\n"

    content += "\n"
    return content


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--output-dir", required=False)
    args = parser.parse_args()

    catalog = SemanticCatalog(args.catalog)

    output_dir = Path(
        args.output_dir or f"docs/{catalog.source_system.lower()}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    write_readme(catalog, output_dir)
    write_entity_pages(catalog, output_dir)
    write_relationships(catalog, output_dir)

    print(f"Wrote Markdown documentation to {output_dir}")


if __name__ == "__main__":
    main()