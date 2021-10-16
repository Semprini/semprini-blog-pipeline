from enum import Enum
from typing import Optional, get_type_hints


class CustomerStatus(Enum):
    New = 0
    Active = 1
    Closed = 2


class Gender(Enum):
    Female = 0
    Male = 1
    Nonbinary = 2


class MaritalStatus(Enum):
    Single = 1
    Married = 2


class PartyType(Enum):
    Individual = 0
    Organisation = 1


class Serializable:
    def to_dict(self):
        type_dict: dict = get_type_hints(type(self))
        output = {}

        for attr_name in type_dict.keys():
            # Attribute must be set on instance
            if hasattr(self, attr_name):
                value = getattr(self, attr_name)
                if isinstance(value, Enum):
                    output[attr_name] = value.name
                else:
                    output[attr_name] = value
        
        return output


class Individual(Serializable):
    id: Optional[str]
    name: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    middle_names: Optional[str]
    prefix: Optional[str]
    gender: Optional[Gender]
    marital_status: Optional[MaritalStatus]


class Customer(Serializable):
    customer_number: Optional[str]
    status = Optional[CustomerStatus]
    party_id = Optional[str]
    party_type = Optional[PartyType]
