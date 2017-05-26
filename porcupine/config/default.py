import tempfile

temp_folder = tempfile.gettempdir()

default_settings = {
    'host': '0.0.0.0',
    'port': 8000,
    'workers': 1,
    'temp_folder': temp_folder,
    'pid_file': '{0}/porcupine.pid'.format(temp_folder),
    'db': {
        'type': 'porcupine.connectors.Couchbase',
        'hosts': ['localhost'],
        # 'protocol': 'couchbase',
        'bucket': 'porcupine',
        'password': '',

        # how many times a transaction is retried
        # before an error is raised
        'txn_max_retries': 16,
        'multi_fetch_chunk_size': 500,
        # dirtiness threshold
        'collection_compact_threshold': 0.3,
        # split threshold set to 64K
        'collection_split_threshold': 64 * 1024,

        # indexes map - maintained by the system
        '__indices__': {},
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
        # keep log for up to 1 week
        'when': 'D',
        'interval': 1,
        'backups': 7,
        'date_format': '%Y-%m-%d %H:%M:%S',
        'format': '%(asctime)s [%(levelname)s][%(processName)s]: %(message)s',

        # access log
        'access_log': False,
        'access_format': '%(asctime)s [%(levelname)s][%(host)s]: ' +
                         '%(request)s %(message)s %(status)d %(byte)d',
    }
}
