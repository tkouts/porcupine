default_settings = {
    'host': '0.0.0.0',
    'port': 8000,
    'workers': 1,
    'daemon': False,
    'temp_folder': '/tmp',
    'db': {
        # how many times a transaction is retried
        # before an error is raised
        'type': 'porcupine.connectors.Couchbase',
        'hosts': ['localhost'],
        # 'protocol': 'couchbase',
        'bucket': 'porcupine',
        'password': '',

        'txn_max_retries': 16,
        'multi_fetch_chunk_size': 500,
        'collection_compact_threshold': 0.3,
        # split threshold set to 64K
        'collection_split_threshold': 65535,
        # optional for running unit tests suite
        # 'tests_bucket': 'porcupine_tests'
    },
    'session_manager': {
        'type': 'porcupine.session.cookie.SessionManager',
        'timeout': 1200,
        'guest_user_id': None,
        'params': {'secret': 'SECRET'}
    },
    'template_languages': {
        'string_template': 'porcupine.core.templates.string_template',
        'normal_template': 'porcupine.core.templates.normal_template',
    },
    'log': {
        # 10 - DEBUG
        # 20 - INFO
        # 30 - WARNING
        # 40 - ERROR
        # 50 - CRITICAL
        'level': 20,
        'max_bytes': 0,
        'backups': 3,
        'format': '%(asctime)s %(processName)s [%(levelname)s] %(message)s',
        # 'mp_format': '%(asctime)s [%(levelname)s/%(processName)s] %(message)s'
    }
}
