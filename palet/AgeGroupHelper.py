from typing import Any


class AgeGroupHelper() :

    # this is an overrideable function in Python that will loop through attributes
    # and allow you to create custom logic
    # in this case __getatrr__ is only called when an attribute you try to call on the object doesn't exist
    # you then can provide logic to do anything; in this case we look up the passed state/name 
    # TODO: Currently this class must have an instance. A couple of ways we might do it:
    #           * Add it in the library initialization so they have access to a State object like State = State()
    #           * Is there a way we can do something in the super class or a loadtime?
    def __getattr__(self, name: str) -> Any:
            return str(self.properties[name])

    properties = {
        "Child": "0-18",
        "YoungAdult": "19-44",
        "Adult": "45-64",
        "Senior": "65-84",
        "Elderly": "84-120"

    }
