import abc
from porcupine.exceptions import SchemaError


class BaseIndex(metaclass=abc.ABCMeta):
    system_attrs = {
        'content_class': '_cc',
        'is_collection': '_col'
    }

    def __init__(self, connector, attr_name, container_types):
        self.connector = connector

        if attr_name in self.system_attrs:
            # system attribute
            self.name = attr_name
            self.key = self.system_attrs[attr_name]
        else:
            # gather data types
            data_types = set()
            attr_path = attr_name.split('.')
            for container_type in container_types:
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
            self.name = attr_name
            self.key = '.'.join([storage_keys[0]] + attr_path[1:])
            self.immutable = all([dt.immutable for dt in data_types])
        self.container_types = self.get_all_subclasses(container_types)

    @staticmethod
    def get_all_subclasses(cls_list) -> dict:
        all_subs = {}
        for cls in cls_list:
            all_subs.update({
                cls: None
                for cls in BaseIndex.get_all_subclasses(cls.__subclasses__())
            })
            all_subs[cls] = None
        return all_subs

    @abc.abstractmethod
    def get_cursor(self, **options):
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError
