from .default import default_settings

settings = default_settings


def add_index(data_type):
    index_map = settings['db']['__indices__']
    # TODO: check duplicates
    index_map[data_type.name] = data_type
