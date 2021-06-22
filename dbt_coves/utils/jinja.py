from jinja2 import Environment, FileSystemLoader, PackageLoader, ChoiceLoader, BaseLoader
from dbt_coves.utils.log import log_manager


def render_template_file(name, context, output_path, templates_folder="templates"):
    env = Environment(
            loader=ChoiceLoader([
                PackageLoader("dbt_coves"),
                FileSystemLoader(templates_folder)
            ])
        )
    template = env.get_template(name)
    output = template.render(**context)

    with open(output_path, "w") as rendered:
        rendered.write(output)
    
    return output

def render_template(template_content, context):
    template = Environment(loader=BaseLoader()).from_string(template_content)
    return template.render(**context)
