class IdentifiableEntity():
    def __init__(self, id:str):
        self.id = id
    
    def getId(self):
        return self.id


class Person(IdentifiableEntity):
    def __init__(self, id:str, name:str):
        super().__init__(id)
        self.name = name

    def getName(self):
        return self.name
    
class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, id:str, title: str, date: str, owner: str, place: str, hasAuthor: list):
        super().__init__(id)
        self.title = title
        self.date = date
        self.owner = owner
        self.place = place
        self.authors = hasAuthor
    
    def getTitle(self):
        return self.title
    def getDate(self):
        return self.date
    def getOwner(self):
        return self.owner
    def getPlace(self):
        return self.place
    def getAuthors(self):
        return self.authors

class NauticalChart(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str, owner: str, place: str, hasAuthor: list):
        super().__init__(id, title, date, owner, place, hasAuthor)

class ManuscriptPlace(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str, owner: str, place: str, hasAuthor: list):
        super().__init__(id, title, date, owner, place, hasAuthor)

class ManuscriptVolume(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str, owner: str, place: str, hasAuthor: list):
        super().__init__(id, title, date, owner, place, hasAuthor)

class PrintedVolume(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str, owner: str, place: str, hasAuthor: list):
        super().__init__(id, title, date, owner, place, hasAuthor)

class PrintedMaterial(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str, owner: str, place: str, hasAuthor: list):
        super().__init__(id, title, date, owner, place, hasAuthor)

class Herbarium(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str, owner: str, place: str, hasAuthor: list):
        super().__init__(id, title, date, owner, place, hasAuthor)

class Specimen(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str, owner: str, place: str, hasAuthor: list):
        super().__init__(id, title, date, owner, place, hasAuthor)

class Painting(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str, owner: str, place: str, hasAuthor: list):
        super().__init__(id, title, date, owner, place, hasAuthor)

class Model(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str, owner: str, place: str, hasAuthor: list):
        super().__init__(id, title, date, owner, place, hasAuthor)
        
class Map(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str, owner: str, place: str, hasAuthor: list):
        super().__init__(id, title, date, owner, place, hasAuthor)