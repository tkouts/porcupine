from porcupine.connectors.base.indexes import FTSIndexBase
from porcupine.connectors.couchbase.ftscursor import FTSCursor


class FTSIndex(FTSIndexBase):
    """
    Couchbase FTS Index
    """
    def get_params(self) -> dict:
        params = dict(
            doc_config=dict(
                mode='type_field',
                type_field='_pcc'
            ),
            mapping=dict(
                default_analyzer='en',
                default_datetime_parser='dateTimeOptional',
                default_field='_all',
                default_mapping=dict(
                    dynamic=True,
                    enabled=False
                ),
                default_type='_default',
                index_dynamic=True,
                store_dynamic=False,
                types={}
            )
        )
        type_params = dict(
            dynamic=False,
            enabled=True,
            properties=dict(
                pid=dict(
                    enabled=True,
                    dynamic=False,
                    fields=[dict(
                        analyzer="keyword",
                        include_in_all=False,
                        include_term_vectors=False,
                        index=True,
                        name='pid',
                        store=False,
                        type='text'
                    )]
                )
            )
        )
        for key in self.keys:
            # split key
            type_mapping = type_params
            keys = key.split('.')
            for nested_key in keys[:-1]:
                type_mapping = type_mapping['properties'].setdefault(
                    nested_key,
                    dict(
                        enabled=True,
                        dynamic=False,
                        properties={}
                    )
                )
            # add
            type_mapping['properties'][keys[-1]] = dict(
                enabled=True,
                dynamic=False,
                fields=[dict(
                    include_in_all=True,
                    include_term_vectors=False,
                    index=True,
                    name=key,
                    store=False,
                    type='text'
                )]
            )
        index_types = params['mapping']['types']
        for container_type in self.all_types:
            index_types[container_type.__name__] = type_params
        return params

    def get_cursor(self, **options):
        return FTSCursor(self, **options)

    def close(self):
        ...
