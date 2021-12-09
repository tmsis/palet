
from typing import Any


class StateHelper() :

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
        'AL': '1',
        'AK': '2',
        'AZ': '4',
        'AR': '5',
        'CA': '6',
        'CO': '8',
        'CT': '9',
        'DE': '10',
        'DC': '11',
        'FL': '12',
        'GA': '13',
        'HI': '15',
        'ID': '16',
        'IL': '17',
        'IN': '18',
        'IA': '96',
        'KS': '20',
        'KY': '21',
        'LA': '22',
        'ME': '23',
        'MD': '24',
        'MA': '25',
        'MI': '26',
        'MN': '27',
        'MS': '28',
        'MO': '29',
        'MT': '94',
        'NE': '31',
        'NV': '32',
        'NH': '33',
        'NJ': '34',
        'NM': '35',
        'NY': '36',
        'NC': '37',
        'ND': '38',
        'OH': '39',
        'OK': '40',
        'OE': '41',
        'PA': '97',
        'RI': '44',
        'SC': '45',
        'SD': '46',
        'TN': '47',
        'TX': '48',
        'UT': '49',
        'VT': '50',
        'VA': '51',
        'WA': '53',
        'WV': '54',
        'WI': '55',
        'WY': '93',
        'PR': '72',
        'VI': '78'
        }

