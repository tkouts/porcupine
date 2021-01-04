import abc
from porcupine import log
from porcupine.exceptions import SchemaError


class BaseIndex(metaclass=abc.ABCMeta):
    system_attrs = {
        'content_class': '_cc',
        'is_collection': '_col'
    }

    def __init__(self, connector, container_type, attr_name):
        self.connector = connector
        self.name = attr_name

        if attr_name in self.system_attrs:
            # system attribute
            self.key = self.system_attrs[attr_name]
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

            # make sure all storage keys are the same
            storage_keys = [dt.storage_key for dt in data_types]
            if len(storage_keys) > 1 and len(set(storage_keys)) > 1:
                raise SchemaError(
                    f'Index {attr_name} references data types '
                    'with diverse storage keys'
                )
            self.key = '.'.join([storage_keys[0]] + attr_path[1:])
            self.immutable = all([dt.immutable for dt in data_types])
        self.container_type = container_type
        self.all_types = self.get_all_subclasses([container_type])

    def get_all_subclasses(self, cls_list) -> dict:
        all_subs = {}
        for cls in cls_list:
            double_indexed_attrs = [
                subclass for subclass in cls.__subclasses__()
                if 'indexes' in subclass.__dict__ and
                   self.name in subclass.indexes
            ]
            if double_indexed_attrs:
                class_names = [c.__name__ for c in double_indexed_attrs]
                log.warn(
                    f'Duplicate index "{self.name}" defined '
                    f'in {", ".join(class_names)}'
                )
            all_subs.update({
                cls: None
                for cls in self.get_all_subclasses(cls.__subclasses__())
            })
            all_subs[cls] = None
        return all_subs

    @abc.abstractmethod
    def get_cursor(self, **options):
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError
