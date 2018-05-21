from porcupine.datatypes import Embedded, String, Composition
from porcupine.schema import Container, Composite


class Test(Composite):
    name = String(required=True)


class Root(Container):
    composition = Composition(accepts=(Test, ))
