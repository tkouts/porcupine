from porcupine.core.schema.container import Container


class RootContainer(Container):
    # containment = Container.containment + (
    #     'org.innoscript.desktop.schema.common.Folder',
    # )

    def get_parent(self, get_lock=True):
        return None


class RecycleBin(Container):
    def get_parent(self, get_lock=True):
        return None
