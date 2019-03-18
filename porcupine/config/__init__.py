import yaml


def parse(config_file):
    with open(config_file, encoding='utf-8') as f:
        return yaml.load(f.read(), Loader=yaml.FullLoader)
