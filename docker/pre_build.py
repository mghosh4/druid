import json
from jinja2 import Environment, FileSystemLoader
import os

TEMPLATE_DIR = "./templates/"
GENERATED_DIR = "./generated/"

# prepare jinja env
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))

# prepare user defined configs
config_file = "./config.json"
with open(config_file, 'r') as fp:
    config = json.load(fp)

# generate directories
if not os.path.exists(GENERATED_DIR):
    os.makedirs(GENERATED_DIR)

os.chdir(TEMPLATE_DIR)
for dp, _, _  in os.walk('./'):
    if not os.path.exists(os.path.join('../', GENERATED_DIR, dp)):
        os.makedirs(os.path.join('../', GENERATED_DIR, dp))

# render all templates
templates = [os.path.join(dp, f) for dp, dn, filenames in os.walk('./') for f in filenames]
os.chdir('../')
for template in templates:
    generated_filename = GENERATED_DIR + template.rsplit(".template")[0]

    with open(GENERATED_DIR + template.rsplit(".template")[0], 'w') as fp:
        if template.endswith('.template'):
            fp.write(env.get_template(template).render(**config))
        else:
            with open(TEMPLATE_DIR + template, 'r') as rofp:
                fp.write(rofp.read())
