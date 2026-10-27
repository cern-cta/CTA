from dataclasses import dataclass
from typing import Annotated, Final, Optional, Union

import pytest
from ctaops.utils.dataclasses import (
    flatten_dataclass_class,
    flatten_dataclass_instance,
)


class FreeText:
    """Marker class for FreeText type in dataclass fields."""


@dataclass
class Apartment:
    number: int
    floor: int

    @property
    def full_address(self) -> Annotated[str, FreeText]:
        return f"{self.number}, {self.floor}"


@dataclass
class Address:
    street: str
    city: str
    zipcode: str
    apartment: Optional[Apartment] = None


@dataclass
class Car:
    brand: str
    immatriculation: str


@dataclass
class CompanyInfo:
    name: str
    industry: str


@dataclass
class Contractor:
    name: Final[str]
    rate: float
    company: Optional[CompanyInfo]
    address: Union[Address, str, None]


@dataclass
class Person:
    name: str
    address: Address
    car: Optional[Car]


@pytest.mark.parametrize(
    "dataclass_instance,max_depth, extra_flatten_fields,include_properties,expected",
    [
        (
            Contractor("Bob Wilson", 120.0, None, Address("456 Oak Ave", "Portland", "97201", None)),
            None,
            (),
            False,
            {
                "name": "Bob Wilson",
                "rate": 120.0,
                "company": None,
                "address.street": "456 Oak Ave",
                "address.city": "Portland",
                "address.zipcode": "97201",
                "address.apartment": None,
            },
        ),
        (
            Contractor(
                "Alice Johnson",
                150.0,
                CompanyInfo("TechCorp", "Software"),
                Address("123 Main St", "Seattle", "98101", Apartment(1, 2)),
            ),
            None,
            (),
            True,
            {
                "name": "Alice Johnson",
                "rate": 150.0,
                "company.name": "TechCorp",
                "company.industry": "Software",
                "address.street": "123 Main St",
                "address.city": "Seattle",
                "address.zipcode": "98101",
                "address.apartment.number": 1,
                "address.apartment.floor": 2,
                "address.apartment.full_address": "1, 2",
            },
        ),
        (
            Contractor("Carol Davis", 175.0, CompanyInfo("DataInc", "Analytics"), None),
            None,
            (),
            False,
            {
                "name": "Carol Davis",
                "rate": 175.0,
                "company.name": "DataInc",
                "company.industry": "Analytics",
                "address": None,
            },
        ),
        (
            Person(
                "Carol Davis", Address("123 Main St", "Seattle", "98101", Apartment(1, 2)), Car("Renault", "01-234-46")
            ),
            1,
            ("car.brand", "address.apartment.number"),
            False,
            {
                "name": "Carol Davis",
                "car.brand": "Renault",
                "address.apartment.number": 1,
            },
        ),
        (
            Person(
                "Carol Davis", Address("123 Main St", "Seattle", "98101", Apartment(1, 2)), Car("Renault", "01-234-46")
            ),
            1,
            ("car.non_existed_field", "non_existed_field", "non_existed.field", "car", "address.car"),
            False,
            {
                "name": "Carol Davis",
                "car.brand": "Renault",
                "car.immatriculation": "01-234-46",
            },
        ),
    ],
)
def test_flatten_dataclass_instance(dataclass_instance, max_depth, extra_flatten_fields, include_properties, expected):
    assert (
        flatten_dataclass_instance(
            dataclass_instance,
            max_depth,
            extra_flatten_fields=extra_flatten_fields,
            include_properties=include_properties,
        )
        == expected
    )


@pytest.mark.parametrize(
    "dataclass_class,max_depth,extra_flatten_fields,include_properties,expected",
    [
        (
            CompanyInfo,
            None,
            (),
            False,
            [
                ("name", frozenset({str})),
                ("industry", frozenset({str})),
            ],
        ),
        (
            Contractor,
            None,
            (),
            True,
            [
                ("name", frozenset({str})),
                ("rate", frozenset({float})),
                ("company.name", frozenset({str})),
                ("company.industry", frozenset({str})),
                ("address.street", frozenset({str})),
                ("address.city", frozenset({str})),
                ("address.zipcode", frozenset({str})),
                ("address.apartment.number", frozenset({int})),
                ("address.apartment.floor", frozenset({int})),
                ("address.apartment.full_address", frozenset({str, FreeText})),
            ],
        ),
        (
            Person,
            None,
            (),
            False,
            [
                ("name", frozenset({str})),
                ("address.street", frozenset({str})),
                ("address.city", frozenset({str})),
                ("address.zipcode", frozenset({str})),
                ("address.apartment.number", frozenset({int})),
                ("address.apartment.floor", frozenset({int})),
                ("car.brand", frozenset({str})),
                ("car.immatriculation", frozenset({str})),
            ],
        ),
        (
            Person,
            1,
            (),
            False,
            [
                ("name", frozenset({str})),
            ],
        ),
        (
            Person,
            2,
            (),
            False,
            [
                ("name", frozenset({str})),
                ("address.street", frozenset({str})),
                ("address.city", frozenset({str})),
                ("address.zipcode", frozenset({str})),
                ("car.brand", frozenset({str})),
                ("car.immatriculation", frozenset({str})),
            ],
        ),
        (
            Person,
            1,
            ("car.brand", "address.apartment.number"),
            False,
            [
                ("name", frozenset({str})),
                ("address.apartment.number", frozenset({int})),
                ("car.brand", frozenset({str})),
            ],
        ),
        (
            Person,
            1,
            ("car.non_existed_field", "non_existed_field", "non_existed.field", "car", "address.car"),
            False,
            [
                ("name", frozenset({str})),
                ("car.brand", frozenset({str})),
                ("car.immatriculation", frozenset({str})),
            ],
        ),
    ],
)
def test_flatten_dataclass_class(dataclass_class, max_depth, extra_flatten_fields, include_properties, expected):
    assert (
        flatten_dataclass_class(
            dataclass_class, max_depth, extra_flatten_fields=extra_flatten_fields, include_properties=include_properties
        )
        == expected
    )
