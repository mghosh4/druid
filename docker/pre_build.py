import json
from jinja2 import Environment, FileSystemLoader
import os
import argparse
import stat

TEMPLATE_DIR = "./templates/"
GENERATED_DIR = "./generated/"
CONFIG_FILE = "./config.json"

def prebuild(is_dryrun):
    # prepare jinja env
    env = Environment(loader=FileSystemLoader('./'))

    # prepare user defined configs
    with open(CONFIG_FILE, 'r') as fp:
        config = json.load(fp)
        config['is_dryrun'] = is_dryrun

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
    scripts_templates = ['build-all.sh.template', 'build-conf-and-scripts.sh.template', 'deploy.sh.template', 'provision.sh.template', 'stop.sh.template']
    for template in templates:
        generated_filename = GENERATED_DIR + template.rsplit(".template")[0]
        with open(generated_filename, 'w') as fp:
            if template.endswith('.template'):
                fp.write(env.get_template(TEMPLATE_DIR + template).render(**config))
            else:
                with open(TEMPLATE_DIR + template, 'r') as rofp:
                    fp.write(rofp.read())

    for template in scripts_templates:
        generated_filename = '../' + template.rsplit(".template")[0]
        with open(generated_filename, 'w') as fp:
            if template.endswith('.template'):
                fp.write(env.get_template(template).render(**config))
            else:
                with open(template, 'r') as rofp:
                    fp.write(rofp.read())
        st = os.stat(generated_filename)
        os.chmod(generated_filename, st.st_mode | stat.S_IEXEC)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Docker Deploy Prebuild')
    parser.add_argument('-d', '--dryrun', action='store_true', help='dryrun(local) mode')
    args = parser.parse_args()
    prebuild(args.dryrun)
