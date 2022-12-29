import os

from jinja2 import ChoiceLoader, Environment, FileSystemLoader, PackageLoader


def add_env_vars(context):
    context["env_vars"] = os.environ.copy()
    return context


def render_template_file(name, context, output_path, templates_folder=".dbt_coves/templates"):
    context_with_env_vars = add_env_vars(context)
    output = get_render_output(name, context_with_env_vars, templates_folder=templates_folder)

    with open(output_path, "w") as rendered:
        rendered.write(output)

    return output


def render_template(template_content, context):
    template = Environment().from_string(template_content)
    context_with_env_vars = add_env_vars(context)
    return template.render(**context_with_env_vars)


def get_render_output(name, context, templates_folder=".dbt_coves/templates"):
    env = Environment(
        loader=ChoiceLoader([FileSystemLoader(templates_folder), PackageLoader("dbt_coves")]),
        keep_trailing_newline=True,
    )
    template = env.get_template(name)
    context_with_env_vars = add_env_vars(context)
    output = template.render(**context_with_env_vars)

    return output
