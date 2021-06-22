from jinja2 import Environment, FileSystemLoader


def render_template(name, context, output_path, templates_folder='templates'):
    env = Environment(loader=FileSystemLoader(templates_folder))
    template = env.get_template(name)
    output = template.render(**context)

    with open(output_path, "w") as rendered:
        rendered.write(output)
    
    return output