from pathlib import Path


def render_template(template_path: str, context: dict) -> str:
    template = Path(template_path).read_text()

    for key, value in context.items():
        template = template.replace(f"{{{{ {key} }}}}", str(value))

    return template.strip()