from porcupine.connectors.base.index import BaseIndex
from porcupine.connectors.couchbase.cursor import Cursor


class FTSIndex(BaseIndex):
    """
    Couchbase FTS index
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
            type_params['properties'][key] = dict(
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
        return Cursor(self, **options)

    def close(self):
        ...
