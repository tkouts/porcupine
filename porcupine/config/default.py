import tempfile

temp_folder = tempfile.gettempdir()

DEFAULTS = {
    # SERVER
    'HOST': '0.0.0.0',
    'PORT': 8000,
    'WORKERS': 1,
    'TEMP_FOLDER': temp_folder,
    'PID_FILE': '{0}/porcupine.pid'.format(temp_folder),


    # DATABASE
    'DB_IF': 'porcupine.connectors.Couchbase',
    'DB_HOST': 'localhost',
    'DB_USER': 'porcupine',
    'DB_PASSWORD': '',
    'DB_CACHE_SIZE': 1000,
    # how many times a transaction is retried
    # before an error is raised
    'DB_TXN_MAX_RETRIES': 16,
    'DB_MULTI_FETCH_SIZE': 500,
    # dirtiness threshold
    'DB_COLLECTION_COMPACT_THRESHOLD': 0.3,
    # split threshold set to 16K
    'DB_COLLECTION_SPLIT_THRESHOLD': 16 * 1024,
    # internal indexes map - maintained by the system
    '__indices__': {},


    # SESSION MANAGER
    'SM_IF': 'porcupine.session.cookie.SessionManager',
    'SM_SESSION_TIMEOUT': 1200,
    'SM_GUEST_USER_ID': None,
    'SM_SECRET': 'SECRET',


    # LOG
    # 10 - DEBUG
    # 20 - INFO
    # 30 - WARNING
    # 40 - ERROR
    # 50 - CRITICAL
    'LOG_LEVEL': 20,
    # keep log for up to 1 week
    'LOG_WHEN': 'D',
    'LOG_INTERVAL': 1,
    'LOG_BACKUPS': 7,
    'LOG_DATE_FORMAT': '%Y-%m-%d %H:%M:%S',
    'LOG_FORMAT': '%(asctime)s [%(levelname)s][%(processName)s]: %(message)s',

    # access log
    'LOG_ACCESS': False,
    'LOG_ACCESS_FORMAT': '%(asctime)s [%(levelname)s][%(host)s]: ' +
                         '%(request)s %(message)s %(status)d %(byte)d'
}
