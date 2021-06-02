import abc
from porcupine import log
from porcupine.exceptions import SchemaError


class SecondaryIndexBase(metaclass=abc.ABCMeta):
    system_attrs = {
        'content_class': '_cc',
        'is_collection': '_col'
    }

    def __init__(self, connector, container_type, attr_list):
        self.connector = connector
        self.attrs = attr_list
        self.name = ','.join(attr_list)

        self.attr_list = tuple(attr_list)
        self.keys = []
        self.defaults = []
        self.immutable = True
        for attr_name in attr_list:
            if attr_name in self.system_attrs:
                # system attribute
                self.keys.append(self.system_attrs[attr_name])
                self.defaults.append(None)
            else:
                # gather data types
                data_types = set()
                attr_path = attr_name.split('.')
                # for container_type in container_types:
                children_types = self.get_all_subclasses(
                    container_type.containment)
                top_level_attr = attr_path[0]
                container_data_types = [
                    child_type.__schema__[top_level_attr]
                    for child_type in children_types
                    if top_level_attr in child_type.__schema__
                ]
                if len(container_data_types) == 0:
                    raise SchemaError(
                        f'Cannot locate indexed attribute "{attr_name}" '
                        f'in container type "{container_type.__name__}"'
                    )
                data_types.update(container_data_types)

                # make sure all storage keys are the same and have same defaults
                storage_keys = [dt.storage_key for dt in data_types]
                defaults = [dt.default for dt in data_types]
                if len(storage_keys) > 1 and len(set(storage_keys)) > 1:
                    raise SchemaError(
                        f'Index {attr_name} references data types '
                        'with diverse storage keys'
                    )
                if len(defaults) > 1 and len(set(defaults)) > 1:
                    raise SchemaError(
                        f'Index {attr_name} references data types '
                        'with diverse default values'
                    )
                self.defaults.append(defaults[0])
                self.keys.append('.'.join([storage_keys[0]] + attr_path[1:]))
                self.immutable = self.immutable and \
                    all([dt.immutable for dt in data_types])

        self.container_type = container_type
        self.all_types = self.get_all_subclasses([container_type])

    def get_all_subclasses(self, cls_list) -> dict:
        all_subs = {}
        for cls in cls_list:
            index_attr = self.index_attr
            for attr_name in self.attrs:
                double_indexed_attrs = [
                    subclass for subclass in cls.__subclasses__()
                    if index_attr in subclass.__dict__ and
                    attr_name in getattr(subclass, index_attr)
                ]
                if double_indexed_attrs:
                    class_names = [c.__name__ for c in double_indexed_attrs]
                    log.warn(
                        f'Duplicate index "{attr_name}" defined '
                        f'in {", ".join(class_names)}'
                    )
            all_subs.update({
                cls: None
                for cls in self.get_all_subclasses(cls.__subclasses__())
            })
            all_subs[cls] = None
        return all_subs

    @property
    def container_name(self):
        return self.container_type.__name__

    @property
    def index_attr(self):
        return 'indexes'

    @abc.abstractmethod
    def get_cursor(self, **options):
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError


class FTSIndexBase(SecondaryIndexBase, metaclass=abc.ABCMeta):
    @property
    def index_attr(self):
        return 'fts_indexes'
