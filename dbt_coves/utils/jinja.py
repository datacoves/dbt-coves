from jinja2 import BaseLoader, ChoiceLoader, Environment, FileSystemLoader, PackageLoader


def render_template_file(name, context, output_path, templates_folder="templates"):
    env = Environment(
        loader=ChoiceLoader([FileSystemLoader(templates_folder), PackageLoader("dbt_coves")]),
        keep_trailing_newline=True
    )
    template = env.get_template(name)
    output = template.render(**context)

    with open(output_path, "w") as rendered:
        rendered.write(output)

    return output


def render_template(template_content, context):
    template = Environment(loader=BaseLoader()).from_string(template_content)
    return template.render(**context)
