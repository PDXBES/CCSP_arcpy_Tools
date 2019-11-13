class DataIoException(Exception):
    pass

class FieldNamesLengthDoesNotMatchRowLengthException(DataIoException):
    pass


class AddObjectException(DataIoException):
    pass

class AddSimulationException(AddObjectException):
    pass

class AddModelAlterationException(AddObjectException):
    pass

class AddProjectTypeException(AddObjectException):
    pass

class AddModelException(AddObjectException):
    pass